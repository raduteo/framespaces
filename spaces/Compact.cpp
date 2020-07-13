
//#include "arrow/array/concatenate.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>


#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"
#include "Compact.h"

namespace framespaces {
using namespace arrow;

/// offset, length pair for representing a Range of a buffer or array
    struct Range {
        int64_t offset = -1, length = 0;

        Range() = default;

        Range(int64_t o, int64_t l) : offset(o), length(l) {}
    };

/// non-owning view into a range of bits
    struct Bitmap {
        Bitmap() = default;

        Bitmap(const uint8_t *d, Range r) : data(d), range(r) {}

        explicit Bitmap(const std::shared_ptr<Buffer> &buffer, Range r)
                : Bitmap(buffer ? buffer->data() : nullptr, r) {}

        const uint8_t *data = nullptr;
        Range range;

        bool AllSet() const { return data == nullptr; }
    };

// Allocate a buffer and concatenate bitmaps into it.
    static Status ConcatenateBitmaps(const std::vector<Bitmap> &bitmaps, MemoryPool *pool,
                                     std::shared_ptr<Buffer> *out) {
        int64_t out_length = 0;
        for (const auto &bitmap : bitmaps) {
            out_length += bitmap.range.length;
        }
        ARROW_ASSIGN_OR_RAISE(*out, AllocateBitmap(out_length, pool));
        uint8_t *dst = (*out)->mutable_data();

        int64_t bitmap_offset = 0;
        for (auto bitmap : bitmaps) {
            if (bitmap.AllSet()) {
                BitUtil::SetBitsTo(dst, bitmap_offset, bitmap.range.length, true);
            } else {
                internal::CopyBitmap(bitmap.data, bitmap.range.offset, bitmap.range.length, dst,
                                     bitmap_offset);
            }
            bitmap_offset += bitmap.range.length;
        }

        // finally (if applicable) zero out any trailing bits
        if (auto preceding_bits = BitUtil::kPrecedingBitmask[out_length % 8]) {
            dst[out_length / 8] &= preceding_bits;
        }
        return Status::OK();
    }

    // Write offsets in src into dst, adjusting them such that first_offset
// will be the first offset written.
    template<typename Offset, typename Length>
    static Status PutOffsets(const Buffer &src, Offset first_offset,
                             Offset *dst, Offset &offset, Length &length);

// Concatenate buffers holding offsets into a single buffer of offsets,
// also computing the ranges of values spanned by each buffer of offsets.
    template<typename Offset>
    static Status ConcatenateOffsets(const BufferVector &buffers, MemoryPool *pool,
                                     std::shared_ptr<Buffer> *out,
                                     std::vector<Range> *values_ranges) {
        values_ranges->resize(buffers.size());

        // allocate output buffer
        int64_t out_length = 0;
        for (const auto &buffer : buffers) {
            out_length += buffer->size() / sizeof(Offset);
        }
        ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer((out_length + 1) * sizeof(Offset), pool));
        auto dst = reinterpret_cast<Offset *>((*out)->mutable_data());

        int64_t elements_length = 0;
        Offset values_length = 0;
        for (size_t i = 0; i < buffers.size(); ++i) {
            // the first offset from buffers[i] will be adjusted to values_length
            // (the cumulative length of values spanned by offsets in previous buffers)
            Offset offset;
            RETURN_NOT_OK(PutOffsets(*buffers[i], values_length, &dst[elements_length],
                                     offset, values_ranges->at(i).length));
            values_ranges->at(i).offset = offset;
            elements_length += buffers[i]->size() / sizeof(Offset);
            values_length += static_cast<Offset>(values_ranges->at(i).length);
        }

        // the final element in dst is the length of all values spanned by the offsets
        dst[out_length] = values_length;
        return Status::OK();
    }

    template<typename Offset, typename Length>
    static Status PutOffsets(const Buffer &src, Offset first_offset,
                             Offset *dst, Offset &offset, Length &length) {
        // Get the range of offsets to transfer from src
        auto src_begin = reinterpret_cast<const Offset *>(src.data());
        auto src_end = reinterpret_cast<const Offset *>(src.data() + src.size());

        // Compute the range of values which is spanned by this range of offsets
        offset = src_begin[0];
        length = *src_end - offset;
        if (first_offset > std::numeric_limits<Offset>::max() - length) {
            return Status::Invalid("offset overflow while concatenating arrays");
        }

        // Write offsets into dst, ensuring that the first offset written is
        // first_offset
        auto adjustment = first_offset - src_begin[0];
        std::transform(src_begin, src_end, dst,
                       [adjustment](Offset offset) { return offset + adjustment; });
        return Status::OK();
    }

    struct LargeCompactionIndex {
        int64_t offset = 0;
        int64_t rangeLen = 0;
        int16_t arrayNumber = 0;

        LargeCompactionIndex() {};

        LargeCompactionIndex(int64_t offset,
                             int64_t rangeLen,
                             int16_t arrayNumber) : offset(offset), rangeLen(rangeLen), arrayNumber(arrayNumber) {

        }
    };

    template<class COMPACTION_INDEX>
    class CompactImpl {
    public:
        CompactImpl(const std::vector<std::shared_ptr<const ArrayData>> &in,
                    const std::vector<COMPACTION_INDEX> &compactionIndex,
                    MemoryPool *pool)
                : in_(std::move(in)), pool_(pool), out_(std::make_shared<ArrayData>()),
                  compactionIndex_(compactionIndex) {
            out_->type = in[0]->type;
            out_->null_count = kUnknownNullCount;
            for (const auto &item : compactionIndex) {
                out_->length += item.rangeLen;
            }
            out_->buffers.resize(in[0]->buffers.size());
            out_->child_data.resize(in[0]->child_data.size());
            for (auto &data : out_->child_data) {
                data = std::make_shared<ArrayData>();
            }
        }

        Result<std::shared_ptr<Buffer>> CompactBuffers(size_t index, const FixedWidthType &fixed, MemoryPool *pool) {
            int64_t out_length = 0;
            int byte_width = fixed.bit_width() / 8;
            for (const auto &item : compactionIndex_) {
                out_length += item.rangeLen * byte_width;
            }
            ARROW_ASSIGN_OR_RAISE(auto out, AllocateBuffer(out_length, pool));
            auto out_data = out->mutable_data();

            for (const auto &item : compactionIndex_) {
                const std::shared_ptr<const ArrayData> &array_data = in_[item.arrayNumber];
                const auto &buffer = array_data->buffers[index];
                if (buffer != nullptr) {
                    size_t usedSize = item.rangeLen * byte_width;
                    std::memcpy(out_data, buffer->data() + (array_data->offset + item.offset) * byte_width,
                                usedSize);
                    out_data += usedSize;
                }
            }
            return std::move(out);
        }


        Status Compact(std::shared_ptr<ArrayData> *out) &&{
            if (out_->null_count != 0) {
                RETURN_NOT_OK(ConcatenateBitmaps(Bitmaps(0), pool_, &out_->buffers[0]));
            }
            RETURN_NOT_OK(VisitTypeInline(*out_->type, this));
            *out = std::move(out_);
            return Status::OK();
        }

        Status Visit(const NullType &) { return Status::OK(); }

        Status Visit(const BooleanType &) {
            return ConcatenateBitmaps(Bitmaps(1), pool_, &out_->buffers[1]);
        }

        Status Visit(const FixedWidthType &fixed) {
            // handles numbers, decimal128, fixed_size_binary
            return CompactBuffers(1, fixed, pool_).Value(&out_->buffers[1]);
        }

        Status Visit(const BinaryType &) {
            std::vector<Range> value_ranges;
            RETURN_NOT_OK(ConcatenateOffsets<int32_t>(Buffers(1, sizeof(int32_t)), pool_,
                                                      &out_->buffers[1], &value_ranges));

            return ConcatenateBuffers(Buffers(2, value_ranges), pool_).Value(&out_->buffers[2]);
        }

        Status Visit(const LargeBinaryType &) {
            std::vector<Range> value_ranges;
            RETURN_NOT_OK(ConcatenateOffsets<int64_t>(Buffers(1, sizeof(int64_t)), pool_,
                                                      &out_->buffers[1], &value_ranges));
            return ConcatenateBuffers(Buffers(2, value_ranges), pool_).Value(&out_->buffers[2]);
        }

        Status Visit(const ListType &) {
            std::vector<CompactionIndex> valueCompactionIndex;
            RETURN_NOT_OK(OffsetsAsCompactionIndex(1, pool_, &out_->buffers[1], valueCompactionIndex));

            return CompactImpl<CompactionIndex>(RawChildDataArrays(0), valueCompactionIndex, pool_).Compact(
                    &out_->child_data[0]);
        }

        Status Visit(const LargeListType &) {
            std::vector<LargeCompactionIndex> valueCompactionIndex;
            RETURN_NOT_OK(OffsetsAsCompactionIndex(1, pool_, &out_->buffers[1], valueCompactionIndex));
            return CompactImpl<LargeCompactionIndex>(RawChildDataArrays(0), valueCompactionIndex, pool_)
                    .Compact(&out_->child_data[0]);
        }

        Status Visit(const FixedSizeListType &listType) {
            std::vector<LargeCompactionIndex> valueCompactionIndex;
            valueCompactionIndex.reserve(compactionIndex_.size());
            for (const auto &item : compactionIndex_) {
                valueCompactionIndex.emplace_back(item.offset * listType.list_size(),
                                                  item.rangeLen * listType.list_size(), item.arrayNumber);
            }
            return CompactImpl<LargeCompactionIndex>(ChildDataArrays(0), valueCompactionIndex, pool_).Compact(
                    &out_->child_data[0]);
        }

        Status Visit(const StructType &s) {
            for (int i = 0; i < s.num_fields(); ++i) {
                RETURN_NOT_OK(
                        CompactImpl(ChildDataArrays(i), compactionIndex_, pool_).Compact(&out_->child_data[i]));
            }
            return Status::OK();
        }

        Status Visit(const DictionaryType &d) {
            auto fixed = internal::checked_cast<const FixedWidthType *>(d.index_type().get());

            // Two cases: all the dictionaries are the same, or unification is
            // required
            bool dictionaries_same = true;
            std::shared_ptr<Array> dictionary0 = MakeArray(in_[0]->dictionary);
            for (size_t i = 1; i < in_.size(); ++i) {
                if (!MakeArray(in_[i]->dictionary)->Equals(dictionary0)) {
                    dictionaries_same = false;
                    break;
                }
            }

            if (dictionaries_same) {
                out_->dictionary = in_[0]->dictionary;
                return ConcatenateBuffers(Buffers(1, *fixed), pool_).Value(&out_->buffers[1]);
            } else {
                return Status::NotImplemented("Concat with dictionary unification NYI");
            }
        }

        Status Visit(const UnionType &u) {
            return Status::NotImplemented("concatenation of ", u);
        }

        Status Visit(const ExtensionType &e) {
            // XXX can we just concatenate their storage?
            return Status::NotImplemented("concatenation of ", e);
        }

    private:
        // Gather the index-th buffer of each input into a vector.
        // Bytes are sliced with the explicitly passed ranges.
        // Note that BufferVector will not contain the buffer of in_[i] if it's
        // nullptr.
        BufferVector Buffers(size_t index, const std::vector<Range> &ranges) {
                    DCHECK_EQ(compactionIndex_.size(), ranges.size());
            BufferVector buffers;
            buffers.reserve(compactionIndex_.size());
            for (size_t i = 0; i < compactionIndex_.size(); ++i) {
                const auto &buffer = in_[compactionIndex_[i].arrayNumber]->buffers[index];
                if (buffer != nullptr) {
                    buffers.push_back(SliceBuffer(buffer, ranges[i].offset, ranges[i].length));
                } else {
                    DCHECK_EQ(ranges[i].length, 0);
                }
            }
            return buffers;
        }

        // Gather the index-th buffer of each input into a vector.
        // Buffers are assumed to contain elements of the given byte_width,
        // those elements are sliced with that input's offset and length.
        // Note that BufferVector will not contain the buffer of in_[i] if it's
        // nullptr.
        BufferVector Buffers(size_t index, int byte_width) {
            BufferVector buffers;
            buffers.reserve(compactionIndex_.size());
            for (const auto &item: compactionIndex_) {
                const std::shared_ptr<const ArrayData> &array_data = in_[item.arrayNumber];
                const auto &buffer = array_data->buffers[index];
                if (buffer != nullptr) {
                    buffers.push_back(SliceBuffer(buffer, (array_data->offset + item.offset) * byte_width,
                                                  item.rangeLen * byte_width));
                }
            }
            return buffers;
        }

        template<class OUT_COMPACTION_INDEX>
        Status OffsetsAsCompactionIndex(size_t index, MemoryPool *pool,
                                        std::shared_ptr<Buffer> *out,
                                        std::vector<OUT_COMPACTION_INDEX> &offsetIndex) {
            using Offset = std::remove_cvref_t<decltype(std::declval<OUT_COMPACTION_INDEX>().offset)>;
            int byte_width = sizeof(Offset);
            offsetIndex.resize(compactionIndex_.size());
            // allocate output buffer
            int64_t out_length = 0;
            for (const auto &item: compactionIndex_) {
                const std::shared_ptr<const ArrayData> &array_data = in_[item.arrayNumber];
                const auto &buffer = array_data->buffers[index];
                if (buffer != nullptr) {
                    out_length += item.rangeLen;
                }
            }
            ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer((out_length + 1) * sizeof(Offset), pool));
            auto dst = reinterpret_cast<Offset *>((*out)->mutable_data());
            int64_t elements_length = 0;
            Offset values_length = 0;

            for (size_t i = 0; i < compactionIndex_.size(); i++) {
                auto &item = compactionIndex_[i];
                const std::shared_ptr<const ArrayData> &array_data = in_[item.arrayNumber];
                const auto &buffer = array_data->buffers[index];
                if (buffer != nullptr) {
                    Buffer sBuffer(buffer, (array_data->offset + item.offset) * byte_width,
                                   item.rangeLen * byte_width);
                    RETURN_NOT_OK(PutOffsets(sBuffer, values_length, &dst[elements_length],
                                             offsetIndex[i].offset, offsetIndex[i].rangeLen));
                    offsetIndex[i].arrayNumber = item.arrayNumber;
                    elements_length += sBuffer.size() / sizeof(Offset);
                    values_length += static_cast<Offset>(offsetIndex[i].rangeLen);
                }
            }

            // the final element in dst is the length of all values spanned by the offsets
            dst[out_length] = values_length;
            return Status::OK();
        }

        // Gather the index-th buffer of each input into a vector.
        // Buffers are assumed to contain elements of fixed.bit_width(),
        // those elements are sliced with that input's offset and length.
        // Note that BufferVector will not contain the buffer of in_[i] if it's
        // nullptr.
        BufferVector Buffers(size_t index, const FixedWidthType &fixed) {
                    DCHECK_EQ(fixed.bit_width() % 8, 0);
            return Buffers(index, fixed.bit_width() / 8);
        }

        // Gather the index-th buffer of each input as a Bitmap
        // into a vector of Bitmaps.
        std::vector<Bitmap> Bitmaps(size_t index) {
            std::vector<Bitmap> bitmaps(compactionIndex_.size());
            for (size_t i = 0; i < compactionIndex_.size(); ++i) {
                auto &compactionIndex = compactionIndex_[i];
                Range range(compactionIndex.offset + in_[compactionIndex.arrayNumber]->offset,
                            compactionIndex.rangeLen);
                bitmaps[i] = Bitmap(in_[compactionIndex.arrayNumber]->buffers[index], range);
            }
            return bitmaps;
        }

        std::vector<std::shared_ptr<const ArrayData>> ChildDataArrays(size_t index) {

            std::vector<std::shared_ptr<const ArrayData>> child_data(in_.size());
            for (size_t i = 0; i < in_.size(); ++i) {
                child_data[i] = in_[i]->child_data[index]->Slice(in_[i]->offset, in_[i]->length);
            }
            return child_data;
        }

        std::vector<std::shared_ptr<const ArrayData>> RawChildDataArrays(size_t index) {

            std::vector<std::shared_ptr<const ArrayData>> child_data(in_.size());
            for (size_t i = 0; i < in_.size(); ++i) {
                child_data[i] = in_[i]->child_data[index];
            }
            return child_data;
        }

        const std::vector<std::shared_ptr<const ArrayData>> &in_;
        MemoryPool *pool_;
        std::shared_ptr<ArrayData> out_;
        std::vector<COMPACTION_INDEX> compactionIndex_;
    };

    Result<std::shared_ptr<Array>>
    Compact(const ArrayVector &arrays, const std::vector<CompactionIndex> &compactionIndex,
            MemoryPool *pool /*= default_memory_pool()*/) {
        if (arrays.size() == 0) {
            return Status::Invalid("Must pass at least one array");
        }

        // gather ArrayData of input arrays
        std::vector<std::shared_ptr<const ArrayData>> data(arrays.size());
        for (size_t i = 0; i < arrays.size(); ++i) {
            if (!arrays[i]->type()->Equals(*arrays[0]->type())) {
                return Status::Invalid("arrays to be concatenated must be identically typed, but ",
                                       *arrays[0]->type(), " and ", *arrays[i]->type(),
                                       " were encountered.");
            }
            data[i] = arrays[i]->data();
        }

        std::shared_ptr<ArrayData> out_data;
        RETURN_NOT_OK(CompactImpl<CompactionIndex>(data, compactionIndex, pool).Compact(&out_data));
        return MakeArray(std::move(out_data));
    }


}  // namespace framespaces

