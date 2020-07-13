#ifndef EXPERIMENTS_FRAMESPACEDECL_H
#define EXPERIMENTS_FRAMESPACEDECL_H

#include <arrow/table.h>
#include <map>
#include <unordered_map>
#include "../BuilderDecl.h"
#include "FrameSpaceFwd.h"
#include "Compact.h"

namespace framespaces {

    inline constexpr size_t SPACE_BLOCK_SIZE = 1024;

    /* Arrow DataFrame Underlying structure
     *
     * Goals
     *
     * The intention of this headers is to address an arrow based DataFrame implementation in a way that permits:
     *  - Addressing data beyond the process memory capacity, by allowing transparent access to on-storage, remote, lazy
     *  - Thread safe read-write access to DataFrames
     *  - Preserve “direct memory-like” data access performance
     *
     * Column Space concept
     *
     * A column space of type T is analogous to virtual memory space (https://en.wikipedia.org/wiki/Virtual_memory)
     * with the main distinction that rather than addressing bytes, it is addressing homogenous arrow array elements of type T.
     * Similar to a virtual memory, an address in the ColumnSpace may be:
     *  - unallocated
     *  - mapped to an actual arrow::Array
     *  - fetched as an arrow::Array by lazily invoking an external data provider (such as a file reader or lazily evaluating function)
     *
     *  ReadOnly Property
     *
     *  As a fundamental property, a column space is read only: data available at a given address is guaranteed to not
     *  change for that zone’s lifetime.
     *
     *  New data ranges may be added to the space, either by explicitly associating a new address with an immutable
     *  array or by adding a data provider, but once that data or data provider was associated with an Column Space “pointer”,
     *  repeat access would yield the same result for that pointer’s lifetime.
     *
     *  Note: Column Space is a key concept behind this design proposal, but it doesn’t have an actual class representation
     *  as its functionality is encapsulated by the DataFrameSpace class
     *
     *  Key Constructs defined in this document are:
     *
     *  - DataFrameSpace/SpaceAllocator/DataProvider
     *  - Index
     *  - DataFrame/ImmutableDataFrame/MutableDataFrame
     *  - IndexMutationSession
     *
     *  Recommended ways for reading this header are either:
     *  - Bottom up: starting with DataFrameOperationExamples::puttingItAllTogether
     *  - Top Down: looking at the DataFrameSpace/Index/DataFrame/IndexMutationSession interfaces and the way they connect
     */

    //A mutable table - can very well be the current arrow::Table as it stands
    using MutableTable = arrow::Table;

    //A placeholder type for a Table that cannot be modified by the user
    class ImmutableTable {
    public:
        //An immutable table can be created from an immutable table, via the move constructor -
        //this allows for a shallow copy since the move semantics guarantees data can no longer be altered via the
        //MutableTable
        ImmutableTable(MutableTable &&mutableTable) {}
    };

    /**
     * A proposed data structure that allows for a universal TableReference:
     * The main point is that it provides a unified ways of referring to table data while ensuring a table has only
     * one owner when in mutable state.
     */
    class TableReference {
        std::variant<std::unique_ptr<MutableTable>, std::shared_ptr<ImmutableTable>> data;

    public:
        explicit TableReference(std::unique_ptr<MutableTable> &&src) {
            data = std::move(src);
        }

        TableReference(const TableReference &src) {
            if (data.index() == 0) {
                throw std::logic_error("Cannot copy a mutable table reference - use move semantics");
            } else {
                data = {std::get<1>(data)};
            }
        }

        TableReference(TableReference &&src) = default;

        void makeConst() {
            if (data.index() == 0) {
                data = std::make_shared<ImmutableTable>(std::move(*std::get<0>(data)));
            }
        }
    };

    //A SpacePointer is an address in the DataFrameSpace and consequently an address in each and every subordinated
    //column spaces - effectively SpacePointer identifies a row in the DataFrameSpace
    using SpacePointer = size_t;
    //Identifies the length of row of ranges, typically starting from a SpacePointer
    using RangeLength = uint32_t;

    /**
     * Describes the rows that need to be copied onto a new block as the result of IndexMutationSession defragmentation
     */
    struct TranslationUnit {
        //Pointer to the new block (where previous blocks need to be copied to)
        const SpacePointer targetPointer_;

        TranslationUnit(SpacePointer targetPointer) : targetPointer_(targetPointer) {}

        /**
         * Contains the original locations of row ranges to be copied onto the new block
         */
        std::vector<std::pair<SpacePointer, RangeLength>> sourceRows_;
    };

    /**
     * Contains one entry for each new block allocated by an IndexMutationSession, indicating data that needs storing at
     * that location.
     */
    struct TranslationLog {
        std::vector<TranslationUnit> blockTranslation;
    };


    /**
     *  A DataFrameSpace is an ordered set of parallel Column Spaces (see Column Space story above):
     *  By parallel we imply that a Column Space address is either valid for each Column Source Space or by none of them.
     *
     *  The DataFrameSpace is split in between "Allocated Space" composed of in-memory available arrow arrays/tables and
     *  "Virtual Space" where requests are delegated to external DataProvider objects.
     */
    class DataFrameSpace {
    public:
        using Provider = SpaceProvider<SPACE_BLOCK_SIZE>;
        using Session = typename Provider::AllocationSession;

        class DataProvider;


    private:
        Provider spaceProviderImpl_;
        std::map<size_t, std::shared_ptr<arrow::Table>> blocksMap_;
        std::map<size_t, std::shared_ptr<DataProvider>> providersMap_;

        friend class IndexMutationSession;

        std::unique_ptr<Session> createMutationContext() {
            return spaceProviderImpl_.newAllocationSession();
        }

        //The schema associated with this DataFrameSpace
        std::shared_ptr<arrow::Schema> schema_;
    public:
        /**
         * A DataProvider is an abstract interface for any external source of arrow tables.
         * The DataProvider guarantees the data values remained the same across all subsequent visit function calls
         *
         * One or more DataProvider objects are mapped by DataFrameSpace into the Virtual Space.
         *
         * Effectively all read requests mapping to a DataProvider are delegated to that provider
         */
        class DataProvider {
        public:
            /**
             * Provides access to data for a given range
             * @param visitor - Visitor function used to read the provided data - the function may be invoked one or
             *                  multiple times, delivering requested data in consecutive chunks.
             * @param providerOffset The beginning of the data range, in the DataProvider space (i.e. this offset is
             *                          fully agnostic of corresponding SpacePointer
             * @param rowsCount The numbers of rows to process
             * @param columns The subset of columns to be passed to the visitor function
             */
            virtual void visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor,
                               size_t providerOffset, RangeLength rowsCount,
                               const std::vector<int> &columns) const = 0;

            virtual size_t numRows() const = 0;

        };

    public:

        DataFrameSpace(std::shared_ptr<arrow::Schema> schema) : schema_(schema) {};

        std::shared_ptr<arrow::Schema> &schema() {
            return schema_;
        }

        /**
         * Registers as the de-facto container of the block reserved at targetPointer
         *
         * This method is used primarily as part of the defragmentation process to manage new blocks allocated to
         * compact sparse rows.
         *
         * @param targetPointer A pointer previously allocated using SpaceManager::reserveBlock()
         * @param reference An arrow table that would be made immutable and used as the data source of that block until
         * the pointer is released. The table size is expected to be less than or equal to spaceManager.blockSize()
         */
        void registerData(SpacePointer targetPointer, std::shared_ptr<arrow::Table> &&reference);

        /**
         * Registers as the de-facto container of the block reserved at targetPointer
         *
         * Unlike registerData where input table sizes are capped to allocation blockSize, the input table here can be of
         * any size allowing for light weight mapping of an existing arrow table as a DataFrame
         * @param newRows An arrow table that is expected to be made immutable and is used as the data source of that block until
         * the pointer is released.
         * @return an Index for the newly mapped data range.
         */
        Index registerExternalData(std::shared_ptr<arrow::Table> newRows);

        /**
         * Registers as the de-facto container of the block reserved at targetPointer
         *
         * @param targetPointer A pointer previously allocated using SpaceManager::reserveBlock()
         * @param reference an pointer to an immutable arrow table that would be used as the data source of that block until
         * the pointer is released. The table size is expected to be less than or equal to spaceManager.blockSize()
         */
        void registerData(SpacePointer targetPointer, const std::shared_ptr<ImmutableTable> &reference);

        /**
         * Registers a DataProvider and associates it to a new SpacePointer that would effectively map the corresponding
         * provider data to a virtual space starting a SpacePointer and spanning rowCount rows
         *
         * @param provider - The provider serving virtual data
         * @param rowsCount - The number of rows offered by the provider
         * @return A new SpacePointer mapping the provider data
         */
        Index registerDataProvider(const std::shared_ptr<DataProvider> &provider, size_t rowsCount);

        /**
         * Main accessor method from reading a contiguous data range
         * @param visitor Visitor function used to read the provided data - the function may be invoked one or
         *                multiple times, delivering requested data in consecutive chunks.
         * @param spaceOffset The starting pointer for the requested region
         * @param rowsCount The number of contiguous rows requested
         * @param columns The subset of columns to be read
         */
        void visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor,
                   SpacePointer spaceOffset, RangeLength rowsCount, const std::vector<int> &columns) const;


        /**
         * Applies the translation log produces by an IndexMutationSession:
         * As part of composing a new index out of other indices mapping the current DataFrameSpace rows, the index
         * creation algorithm may chose to copy sparse rows onto new contiguous regions in order to limit fragmentation.
         * This copy operations are recorder in the translationLog, and effectively executed after the index was created
         * but before the index can be used to for a DataFrame pointing to the current space.
         * @param translationLog List of rowRanges to be copied on newly allocated blocks.
         */
        void applyDefragmentation(const TranslationLog &translationLog) {
            for (const auto &translationUnit : translationLog.blockTranslation) {
                //Allocates the actual arrow table that would be mapped to the new block (to be registered as
                // translationUnit.targetPointer)
                std::vector<std::shared_ptr<arrow::ChunkedArray>> newColumns(schema_->num_fields());
                std::vector<arrow::ArrayVector> arraysPerColumn(schema_->num_fields());
                std::vector<CompactionIndex> compactionIndex;
                compactionIndex.reserve(translationUnit.sourceRows_.size());
                std::unordered_map<size_t, uint16_t> firstRowNoArrayNo;
                for (const auto &pointerAndRangeLen : translationUnit.sourceRows_) {
                    size_t globalOffset = pointerAndRangeLen.first;
                    visit([&](const arrow::Table &sourceChunk, size_t offset, uint32_t length) {
                        //NOTE:it is expected that sourceChunk contains on contiguous array for each column - this is an
                        //intrinsic property of a FrameSpace - in order to support columns with different chunking
                        // structure
                        auto columns = sourceChunk.columns();
                        for (const auto &column : columns) {
                            assert(column->num_chunks() == 1);
                        }
                        CompactionIndex currentIndex;
                        if (firstRowNoArrayNo.contains(globalOffset - offset)) {
                            currentIndex.arrayNumber = firstRowNoArrayNo[globalOffset - offset];
                        } else {
                            currentIndex.arrayNumber = arraysPerColumn[0].size();
                            for (size_t i = 0; i < columns.size(); i++) {
                                arraysPerColumn[i].push_back(columns[i]->chunk(0));
                            }
                        }
                        currentIndex.offset = offset;
                        currentIndex.rangeLen = length;
                        compactionIndex.push_back(currentIndex);
                        globalOffset += length;
                    }, pointerAndRangeLen.first, pointerAndRangeLen.second, {});
                    //Once the new block has been initialized, it is added to the data frame space
                    //TODO registerData(translationUnit.targetPointer, TableReference(std::move(newTableBlock)));
                    //NOTE that all the data initialization is done BEFORE the table corresponding to the new block is added
                    //to the data frame space effectively allowing us the guarantee that any row accessible through the
                    //space remains immutable
                }
                for (size_t i = 0; i < schema_->num_fields(); i++) {
                    auto result = Compact(arraysPerColumn[i], compactionIndex);
                    newColumns[i] = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{result.ValueOrDie()});
                }
                registerData(translationUnit.targetPointer_, arrow::Table::Make(schema_, newColumns));
            }
        }

    };


    /**
     * The Index class is used to identify the rows from a DataFrameSpace that form an actual DataFrame
     *
     * It stores a number of ranges <SpacePointer, RangeLength> mapping data to the logical order in the DataFrame.
     * It is read-only
     * It limits fragmentation according to a MAX_FRAGMENTATION constant, where fragmentation is defined as:
     * <contiguous ranges count>/<rows count>, and the constraint is:
     * <contiguous ranges count>/<rows count> < MAX_FRAGMENTATION
     * Effectively this is intended to allow the amortized sequential read time to be comparable to a linear memory read.
     *
     * The actual implementation details and algorithm deserve a separate discussion, but the structure I have in mind
     * is similar to a BTree with immutable nodes. This give us a few important properties:
     *      - log(<ranges count>) positional access
     *      - constant time sequential iteration step
     *      - cache friendly positional access, compared to binary searching a vector or traversing a binary tree:
     *      For example:
     *      Given 1<<24 ranges, for BNodes of size 16-32, the max  b-tree height would be 6, hence we have a
     *      maximum of 6 cache misses to locate a chunk by first row position.
     *      By comparison binary searching an offset vector of size 1<<24, would take 24 steps and likely 20 cache misses
     *      (in both cases we assume 16 consecutive offsets make for a cache line, hence the last 4 steps of a binary
     *      search are likely cache hits).
     *      Similarly locating a key in a binary tree of that size may incur up to 24 misses.
     *
     *      - Node reuse in multiple indices: Indices can be sliced and stitched at S*log(S),where S is the number of
     *      stitches and slices in the constructing operation. A BNode is both immutable and agnostic of its absolute
     *      offset in the tree (it only maintains the relative offsets of its children), allowing for subtree reusal at
     *      potentially different positional offsets.
     */
    class Index {
    public:
        using BuilderT = Builder<size_t, 16, SPACE_BLOCK_SIZE, IndexAdapter>;
    private:
        using IndexImpl = typename BuilderT::VarType;
        using LeafT = BuilderT::LeafT;
        using BlockPointer = typename SpaceProvider<SPACE_BLOCK_SIZE>::RefId;

        IndexImpl impl_;

        friend class DataFrameSpace;

        friend class IndexMutationSession;

        Index(BlockPointer &&refId, size_t len) : impl_(LeafT::createLeafCPtr({std::move(refId), 0, len, len})) {};

        Index(IndexImpl &&impl) : impl_(std::move(impl)) {};
    public:

        Index(std::vector<std::pair<SpacePointer, RangeLength>> initializerList) {
            //Enforcing the MAX_FRAGMENTATION
            double elementCount = 0;
            double rangeCount = 0;
            for (const auto &p: initializerList) {
                rangeCount++;
                elementCount += p.second;
            }
            //TODO assert(rangeCount == 0.0 || (rangeCount / elementCount < MAX_FRAGMENTATION));
            //... init code
        }

        /**
         * Provides linear access to a region of the index
         *
         * The function is expected to complete in O(log(rangeCount)) + O(length)
         * @param visitor The function delivering the current active range to the reader
         * @param offset The positional start of the index operation
         * @param length The number of index positions being read;
         */
        void forEach(std::function<void(SpacePointer, RangeLength)> visitor, size_t offset, size_t length) const;

        /**
         * @return Returns the mapped row count
         */
        size_t size() const {
            return sizeOf(impl_);
        }
    };

    //A data frame interface - A DataFrame can be thought of as an <Index,DataFrameSpace> pair.
    class DataFrame {
    public:
        /**
         * Provides access to data for a given range
         * @param visitor Visitor function used to read the provided data - the function may be invoked one or
         *                multiple times, delivering requested data in consecutive chunks.
         * @param columns The subset of columns to be passed to the visitor function
         * @param offset The position start of the range being accessed
         * @param rowCount The number of rows to process
         */
        virtual void visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor,
                           const std::vector<int> &columns,
                           size_t offset, size_t rowCount) const = 0;

        //The DataFrameSpaces associated with the space
        virtual const std::shared_ptr<DataFrameSpace> &getSpace() const = 0;

        //The Index associated with the space
        virtual const Index &getIndex() const = 0;

        virtual size_t size() const = 0;

        virtual std::shared_ptr<const DataFrame> snapshot() const = 0;
    };

    /**
     * A read only data frame - it effectively composes an index and a DataFrameSpace object.
     */
    class ImmutableDataFrame : public DataFrame, public std::enable_shared_from_this<ImmutableDataFrame> {
        //The index identifying the position of each row
        const Index index_;
        //The space containing the data
        const std::shared_ptr<DataFrameSpace> frameSpace_;
    public:
        ImmutableDataFrame(Index &&index, const std::shared_ptr<DataFrameSpace> &frameSpace) :
                index_(std::move(index)), frameSpace_(frameSpace) {}


        void visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor,
                   const std::vector<int> &columns,
                   size_t offset, size_t rowCount) const override {
            index_.forEach([&](SpacePointer spaceOffset, RangeLength currentRowsCount) {
                frameSpace_->visit(visitor, spaceOffset, currentRowsCount, columns);
            }, offset, rowCount);
        }

        const std::shared_ptr<DataFrameSpace> &getSpace() const override {
            return frameSpace_;
        };

        const Index &getIndex() const override {
            return index_;
        }

        size_t size() const override {
            return index_.size();
        }

        std::shared_ptr<const DataFrame> snapshot() const override {
            return std::static_pointer_cast<const DataFrame>(shared_from_this());
        }

    };

    /**
     * A MutableDataFrame is just a light-weight wrapper around an ImmutableDataFrame
     *
     * I have opted for not relying read/write locking mechanism since that is likely to limit the ability to parallelize
     * operations against it and can be particularly hard to manage in a distributed environment.
     * Consequently thread safety relies on the following constructs:
     * - consistent snapshoting - A reader can request a snapshot that is guaranteed to remain immutable
     * - version tracking - Maintain consecutive versions for each change, which allows the reader to verify whether
     * the data to change since the last snapshot
     * - atomic update - The update operation is a simple pointer redirection together with a version update (yes we lock
     *  for that, but that lock is held for a well known number of instructions)
     * - concurrent modification check - each mutate operation specifies the expected new version which should always be
     * <current version>+1, if that version doesn't match the change is rejected. Effectively that guarantees that a writer
     * is modifying the version it "thinks" it is modifying or else the operation fails (a write is required to check on
     * the current version and data snapshot prior making a change)
     */
    class MutableDataFrame : public DataFrame {
        size_t version_ = 0;
        std::shared_ptr<ImmutableDataFrame> currentFrame_;

        mutable std::mutex mutationMutex_;

    public:

        explicit MutableDataFrame(const std::shared_ptr<ImmutableDataFrame> &initialValue) : currentFrame_(
                initialValue) {
        }

        /**
         * It effectively allows one to swap out the underlying immutable frame and replace it with a new one while
         * doing a concurrent modification check
         * @param newVersion - the expected newVersion is used to prevent concurrent modifications, by confirming the
         * change is really applied to the expected version
         * @param newFrame - A pointer to a new ImmutableDataFrame
         */
        void mutate(size_t newVersion, std::shared_ptr<ImmutableDataFrame> newFrame) {
            std::lock_guard<std::mutex> lock(mutationMutex_);
            if (newVersion != version_ + 1) {
                throw std::invalid_argument("Wrong update version");
            }
            currentFrame_ = std::move(newFrame);
        }

        /**
         * @return The current immutable value and the associated version
         */
        std::pair<size_t, std::shared_ptr<ImmutableDataFrame>> versionAndSnapshot() const {
            std::lock_guard<std::mutex> lock(mutationMutex_);
            return {version_, currentFrame_};
        }

        /**
         * Note: This implementation itself is thread safe
         */
        void visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor,
                   const std::vector<int> &columns,
                   size_t offset, size_t rowCount) const override {
            currentFrame_->visit(visitor, columns, offset, rowCount);
        }

        const std::shared_ptr<DataFrameSpace> &getSpace() const override {
            return currentFrame_->getSpace();
        };

        const Index &getIndex() const override {
            return currentFrame_->getIndex();
        }

        size_t size() const override {
            return currentFrame_->size();
        }

        std::shared_ptr<const DataFrame> snapshot() const override {
            return currentFrame_;
        }
    };

    /**
     * This class provides the foundation for deriving one DataFrame's content from another's.
     * It simply gives the primitives for creating a new Index based of a combination of other indices.
     *
     * A new session is created for each new index construction, it can receive an arbitrary number of addSubIndex calls
     * appending Index slices to the right of the currently accumulated slices. The final index is created by invoking
     * close method. Once close method is called, no other methods can be invoked against this object instance
     *
     * In order to ensure FragmentationConstraint is respected by the result, addSubIndex and close methods may need to
     * do some defragmentation by relocating data pointed by incoming indices to new blocks:
     * Conceptually the mutation session stitches together subranges of rows, as long as average range size is big enough,
     * resulting DataFrame can just point to the same ranges, but if data ranges become too fragmented the new DataFrame
     * read performance would degrade and so it is preferable to copy parts of the fragmented ranges to a newly allocated
     * contiguous area.
     * In practice, the index session is agnostic of the actual DataFrame schema and underlying DataFrameSpace, so index
     * mutation would only create a TranslationLog (a log of the row copying that needs to happen) and produce a Index that points the
     * new ranges.
     */
    class IndexMutationSession {
        using BuilderT = typename Index::BuilderT;
        std::unique_ptr<typename DataFrameSpace::Session> sessionImpl_;
        BuilderT builder_;

        inline static constexpr auto NULL_ENTRY = DataFrameSpace::Provider::NULL_ENTRY;
    public:
        IndexMutationSession(DataFrameSpace &frameSpace) : sessionImpl_(frameSpace.createMutationContext()) {
            builder_.setContext(sessionImpl_.get());
        };

        /**
         * Adds a subRange of an index to the current index accumulation.
         *
         * Because indices a formed of immutable BTree nodes, the time and memory complexity are both O(<tree height>) = O(log(chunks count))
         * Subsequent optimizations can make complexity be amortized min(O(len),O(log(chunks count))), effectively making small range
         * insertions constant in memory and time cost.
         * @param index
         * @param offset
         * @param len
         */
        void addSubIndex(const Index &index, size_t offset, size_t len);

        /**
         * Builds the final version of the index ensuring that it follows the fragmentation constraint specified in
         * Index class definition:
         * rangeCount / elementCount < MAX_FRAGMENTATION
         *
         * @return a pair formed of the resulting index and a TranslationLog corresponding to each new block address by
         * the resulting index as the result of the defragmentation;
         */
        std::pair<Index, TranslationLog> close();

    };

}

//TODO - try to actually implement the DFSpace with support for in memory tables and a parquet driver
//TODO - wrap the index and index builder into the actual index here
//TODO - test insertion and mutation
#endif //EXPERIMENTS_FRAMESPACEDECL_H
