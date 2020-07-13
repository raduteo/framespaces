
#ifndef EXPERIMENTS_ARRAYADAPTER_H
#define EXPERIMENTS_ARRAYADAPTER_H

#include "ArrayAdapterFwd.h"
#include <cstring>
#include "arrow/table.h"
#include <atomic>

template<class T, size_t SIZE>
struct ArrayAdapter {
private:
    using Allocator = StdFixedSizeArrayAllocator<T, SIZE>;
    using Deleter = DeleterForAllocator<T, StdFixedSizeArrayAllocator<T, SIZE>>;
    inline static Allocator &alloc = Allocator::oneAndOnly();
public:
    using ArrayPtr = std::unique_ptr<T[], Deleter>;
    using ArrayCPtr = std::shared_ptr<const T>;
    using ValueType = T;

    using DeclaredType = std::variant<ArrayPtr, ArrayCPtr>;


    static ArrayPtr createLeaf(void *context = nullptr) {
        assert(context == nullptr);
        return ArrayPtr(alloc.allocate(1), Deleter());
    }

    static const T *constArray(const DeclaredType &leaf) {
        if (leaf.index() == 0) {
            return &std::get<0>(leaf)[0];
        } else {
            return &(std::get<1>(leaf).get()[0]);
        }
    }

    static const T &at(const DeclaredType &leaf, size_t pos) {
        if (leaf.index() == 0) {
            return std::get<0>(leaf)[pos];
        } else {
            return std::get<1>(leaf).get()[pos];
        }
    }

    static void copy(DeclaredType &dest, size_t destOffset, const DeclaredType &src, size_t srcOffset, size_t length) {
        if (dest.index() == 0) {
            if (src.index() == 0) {
                memcpy(&std::get<ArrayPtr>(dest)[destOffset], &std::get<ArrayPtr>(src)[srcOffset],
                       length * sizeof(T));
            } else {
                memcpy(&std::get<ArrayPtr>(dest)[destOffset], &std::get<ArrayCPtr>(src).get()[srcOffset],
                       length * sizeof(T));
            }
        } else {
            throw std::logic_error("Cannot write to a const leaf");
        }
    }

    static void getValues(T *destLeaf, const DeclaredType &src, size_t srcOffset, size_t length) {
        const T *data = src.index() ? std::get<ArrayCPtr>(src).get() : static_cast<const T *>(std::get<ArrayPtr>(
                src).get());
        if constexpr (std::is_trivially_copyable<T>::value) {
            memcpy(destLeaf, &data[srcOffset], length * sizeof(T));
        } else {
            for (size_t i = 0; i < length; i++) {
                destLeaf[i] = data[srcOffset + i];
            }
        }
    }

    static void setAt(DeclaredType &leaf, size_t pos, const T &value) {
        std::get<0>(leaf)[pos] = value;
    }

    static void setValues(DeclaredType &dest, size_t offset, const T *srcLeaf, size_t length) {
        T *data = &std::get<ArrayPtr>(dest)[offset];
        if constexpr (std::is_trivially_copyable<T>::value) {
            std::memcpy(data, srcLeaf, length * sizeof(T));
        } else {
            for (size_t i = 0; i < length; i++) {
                data[i] = srcLeaf[i];
            }
        }
    }

    static DeclaredType mutateCopy(const DeclaredType &src) {
        T *resultPointer = alloc.allocate(1);
        getValues(resultPointer, src, 0, SIZE);
        return ArrayPtr(resultPointer, Deleter());
    }

    static void mutate(DeclaredType &leaf, void *context) {
        assert(context == nullptr);
        if (leaf.index() == 1) {
            leaf = mutateCopy(leaf);
        }
    }

    static void makeConst(DeclaredType &leaf) {
        if (leaf.index() == 0) {
            ArrayPtr &pointer = std::get<0>(leaf);
            T *data = pointer.release();
            leaf = DeclaredType(ArrayCPtr(data, Deleter(), alloc));
        }
    }

    static DeclaredType copy(const DeclaredType &leaf) {
        return leaf.index() == 1 ? DeclaredType(std::get<1>(leaf)) : mutateCopy(leaf);
    }

    static void shiftData(DeclaredType &buf, size_t from, size_t to, size_t length) {
        memmove(&std::get<ArrayPtr>(buf)[to], &std::get<ArrayPtr>(buf)[from], length * sizeof(T));
    }

    static bool isMutable(const DeclaredType &buf) { return buf.index() == 0; }

    static bool isNull(const DeclaredType &buf) {
        return buf.index() == 0 ? std::get<0>(buf) == nullptr : std::get<1>(buf) == nullptr;
    }
};

template<class T, size_t SIZE>
struct IndexAdapter;

template<size_t BlockSize>
struct SpaceProvider {
    using Allocator = StdFixedSizeArrayAllocator<size_t, BlockSize>;
    using Deleter = DeleterForAllocator<size_t, Allocator>;
    using CopyList = std::unique_ptr<size_t[], Deleter>;

    inline static constexpr size_t NULL_ENTRY = std::numeric_limits<size_t>::max();


    struct TranslationUnit {

        TranslationUnit(const std::shared_ptr<size_t> &targetPointer, CopyList &&sourceRows) :
                targetPointer_(targetPointer), sourceRows_(std::move(sourceRows)) {}

        //Pointer to the new block (where previous blocks need to be copied to)
        const std::shared_ptr<size_t> targetPointer_;
        /**
         * Contains the original locations of row ranges to be copied onto the new block
         */
        CopyList sourceRows_;
    };


    struct RefId;
    struct RefIdWithTracking;

    class AllocationSession {
        std::deque<TranslationUnit> commitChanges_;

        SpaceProvider &spaceProvider_;
    public:
        AllocationSession(SpaceProvider &spaceProvider) : spaceProvider_(spaceProvider) {}

        auto makeConst(RefIdWithTracking &&refId) -> RefId;

        auto newBlock() -> RefIdWithTracking;

        std::deque<TranslationUnit> close() { return std::move(commitChanges_); }

    };

    struct RefId {
        const size_t firstReference = 0;
        const size_t size;

        RefId(size_t firstReference, size_t size, std::shared_ptr<size_t> ownerPtr) :
                firstReference(firstReference), size(size), ownerPtr_(ownerPtr) {}

        RefId(RefId &&refId) = default;

        RefId(const RefId &refId) = default;

        size_t operator[](size_t pos) {
            return firstReference + pos;
        }

        void getValues(size_t *destLeaf, size_t srcOffset, size_t length) {
            assert(srcOffset <= BlockSize);
            assert(srcOffset + length <= BlockSize);
            for (size_t i = 0; i < length; i++) {
                destLeaf[i] = i + srcOffset + firstReference;
            }
        }

        size_t id() const {
            return *ownerPtr_;
        }

    private:
        friend class SpaceProvider;

        std::shared_ptr<size_t> ownerPtr_;
    };


    struct RefIdWithTracking : public RefId {
        AllocationSession &allocationSession_;

        CopyList copyList;

        RefIdWithTracking(size_t firstReference, size_t size, std::shared_ptr<size_t> ownerPtr,
                          AllocationSession &allocationSession) :
                RefId(firstReference, size, ownerPtr), allocationSession_(allocationSession),
                copyList(copyListAlloc.allocate(1), Deleter()) {
            std::fill(copyList.get(), copyList.get() + BlockSize, NULL_ENTRY);
        }

        size_t &operator[](size_t pos) {
            return copyList[pos];
        }

        void copyFrom(const RefId &src, size_t srcOffset, size_t destOffset, size_t len) {
            assert(destOffset <= BlockSize);
            assert(destOffset + len <= BlockSize);
            assert(srcOffset + len <= src.size);
            for (size_t i = 0; i < len; i++) {
                copyList[i + destOffset] = src.firstReference + i + srcOffset;
            }
        }

        void copyFrom(const RefIdWithTracking &src, size_t srcOffset, size_t destOffset, size_t len) {
            assert(destOffset <= BlockSize);
            assert(destOffset + len <= BlockSize);
            for (size_t i = 0; i < len; i++) {
                copyList[i + destOffset] = src.copyList[srcOffset + i];
            }
        }

        void getValues(size_t *destLeaf, size_t srcOffset, size_t length) {
            assert(srcOffset <= BlockSize);
            assert(srcOffset + length <= BlockSize);
            memcpy(destLeaf, &copyList[srcOffset], length * sizeof(size_t));
        }

        void setValues(const size_t *destLeaf, size_t destOffset, size_t length) {
            assert(destOffset <= BlockSize);
            assert(destOffset + length <= BlockSize);
            memcpy(&copyList[destOffset], destLeaf, length * sizeof(size_t));
        }

        void shiftData(size_t from, size_t to, size_t length) {
            memmove(&copyList[to], &copyList[from], length * sizeof(size_t));
        }
    };


private:
    using BlockAllocator = StdFixedAllocator<size_t>;
    using BlockDeleter = DeleterForFixedAllocator<size_t>;

    struct DeleterForReference {
        SpaceProvider &spaceProvider_;

        DeleterForReference(SpaceProvider &spaceProvider) : spaceProvider_(spaceProvider) {}

        void operator()(size_t *__ptr) {
            spaceProvider_.removingReference(*__ptr);
            auto &allocator = StdFixedAllocator<size_t>::oneAndOnly();
            allocator.destroy(__ptr);
            allocator.deallocate(__ptr, 1);
        }
    };

    std::atomic<size_t> nextId = BlockSize;//Start one step off zero


    inline static Allocator &copyListAlloc = Allocator::oneAndOnly();
public:

    void removingReference(size_t refId) {
        //TODO notify listeners
    }

    size_t nextAddress() {
        auto result = (nextId += BlockSize) - BlockSize;//We want the previous value
        return result;
    }

    RefId mapBlock(size_t size) {
        static auto &blockAllocator = BlockAllocator::oneAndOnly();

        size_t *p = blockAllocator.allocate(1);
        *p = nextId;
        nextId += (size / BlockSize) * BlockSize;
        if (size % BlockSize) {
            nextId += BlockSize;
        }
        return RefId(*p, size, std::shared_ptr<size_t>(p, DeleterForReference(*this)));
    }

    std::unique_ptr<AllocationSession> newAllocationSession() {
        return std::make_unique<AllocationSession>(*this); //propagate and hold
    }
    //TODO - use this with the index adapter
    //Define - ColumnSpace, BIndex, Column,
};

template<size_t BlockSize>
auto SpaceProvider<BlockSize>::AllocationSession::makeConst(RefIdWithTracking &&refId) -> RefId {
    commitChanges_.emplace_back(refId.ownerPtr_, std::move(refId.copyList));
    return RefId(std::move(refId));
}

template<size_t BlockSize>
auto SpaceProvider<BlockSize>::AllocationSession::newBlock() -> SpaceProvider::RefIdWithTracking {
    static auto &blockAllocator = BlockAllocator::oneAndOnly();

    size_t *p = blockAllocator.allocate(1);
    *p = spaceProvider_.nextAddress();
    return RefIdWithTracking(*p, BlockSize, std::shared_ptr<size_t>(p, DeleterForReference(spaceProvider_)), *this);

}


template<size_t SIZE>
struct IndexAdapter<size_t, SIZE> {
    using ValueType = size_t;

private:
    using Provider = SpaceProvider<SIZE>;
    using ProviderSession = typename Provider::AllocationSession;

public:
    using ArrayPtr = typename Provider::RefIdWithTracking;
    using ArrayCPtr = typename Provider::RefId;

    using DeclaredType = std::variant<ArrayPtr, ArrayCPtr>;


    static ArrayPtr createLeaf(void *context = nullptr) {
        assert(context != nullptr);
        return static_cast<ProviderSession *>(context)->newBlock();
    }

    static ValueType at(const DeclaredType &leaf, size_t pos) {
        if (leaf.index() == 0) {
            return std::get<0>(leaf).id() + pos;
        } else {
            return std::get<1>(leaf).id() + pos;
        }
    }

    static void copy(DeclaredType &dest, size_t destOffset, const DeclaredType &src, size_t srcOffset, size_t length) {
        if (dest.index() == 0) {
            if (src.index() == 0) {
                std::get<ArrayPtr>(dest).copyFrom(std::get<ArrayPtr>(src), srcOffset, destOffset, length);
            } else {
                std::get<ArrayPtr>(dest).copyFrom(std::get<ArrayCPtr>(src), srcOffset, destOffset, length);
            }
        } else {
            throw std::logic_error("Cannot write to a const leaf");
        }
    }

    static void getValues(ValueType *destLeaf, const DeclaredType &src, size_t srcOffset, size_t length) {
        std::visit([&](const auto &src) {
            src.getValues(destLeaf, srcOffset, length);
        }, src);
    }

    static void setAt(DeclaredType &leaf, size_t pos, const ValueType &value) {
        std::get<0>(leaf)[pos] = value;
    }

    static void setValues(DeclaredType &dest, size_t offset, const ValueType *srcLeaf, size_t length) {
        std::get<ArrayPtr>(dest).setValues(srcLeaf, offset, length);
    }

    static DeclaredType mutateCopy(const DeclaredType &src, ProviderSession *context) {
        DeclaredType result = DeclaredType(context->newBlock());
        copy(result, 0, src, 0, std::visit([](const auto &ref) {
            return ref.size;
        }, src));
        return result;
    }

    static void mutate(DeclaredType &leaf, void *context) {
        assert(context != nullptr);

        if (leaf.index() == 1) {
            //DeclaredType mutatedCopy = mutateCopy(leaf, static_cast<ProviderSession *>(context));
            leaf.template emplace<ArrayPtr>(
                    std::get<ArrayPtr>(mutateCopy(leaf, static_cast<ProviderSession *>(context))));
        }
    }

    static void makeConst(DeclaredType &leaf) {
        if (leaf.index() == 0) {
            leaf.template emplace<ArrayCPtr>(
                    std::get<0>(leaf).allocationSession_.makeConst(std::get<0>(std::move(leaf))));
        }
    }

    static DeclaredType copy(const DeclaredType &leaf) {
        return leaf.index() == 1 ? DeclaredType(ArrayCPtr(std::get<1>(leaf))) :
               mutateCopy(leaf, &std::get<0>(leaf).allocationSession_);
    }

    static void shiftData(DeclaredType &buf, size_t from, size_t to, size_t length) {
        std::get<ArrayPtr>(buf).shiftData(from, to, length);
    }

    static bool isMutable(const DeclaredType &buf) { return buf.index() == 0; }

    static bool isNull(const DeclaredType &buf) {
        return buf.index() == 0 ? !std::get<0>(buf).firstReference : !std::get<1>(buf).firstReference;
    }
};

#endif //EXPERIMENTS_ARRAYADAPTER_H
