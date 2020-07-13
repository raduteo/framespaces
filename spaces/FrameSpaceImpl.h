#ifndef EXPERIMENTS_FRAMESPACEIMPL_H
#define EXPERIMENTS_FRAMESPACEIMPL_H

#include <arrow/pretty_print.h>
#include "FrameSpaceDecl.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"


namespace framespaces {
    struct DataFrameOperationExamples {

        /*
         * Each table mutation follow these steps (some may be skipped depenting on the operating nam):
         * Step 1. Associate arrow table data with the data frame space
         * Step 2. Create an index for the new data
         * Step 3. Create a result index by combining one or more indices
         * Step 4. Apply any defragmentation by copying data onto the new buffers
         * Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
         */

        /**
         * Creates a data frame from a table
         * @param sourceTable - a unique pointer to an arrow table - since we expect to use it as an immutable data
         * source, we need to ensure there are no other active pointers reaching the table.
         * @return
         */
        static std::shared_ptr<MutableDataFrame> dataFrameFromTable(std::shared_ptr<arrow::Table> sourceTable) {
            auto numRows = sourceTable->num_rows();
            // Step 1. Associate arrow table data with the data frame space
            //Create a DataFrameSpace matching the incoming schema
            auto dataFrameSpace = std::make_shared<DataFrameSpace>(sourceTable->schema());
            //Make the incoming table a constant
            //Register the incoming table with the data frame space and retrieve an associated index
            Index dataIndex = dataFrameSpace->registerExternalData(std::move(sourceTable));
            //(Steps 3,4 do not apply here)
            //Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            //Create the actual data frame by composing the index with the data frame space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(std::move(dataIndex), dataFrameSpace);
            //Creates a mutable data frame having this as the original value
            return std::make_shared<MutableDataFrame>(immutableDataFrame);
        }

        //Placeholder class for a utility that would allow for lazily loading paquet data
        struct MockParquetProvider {
            MockParquetProvider(std::string filePath);

            std::shared_ptr<DataFrameSpace::DataProvider> getDataProvider();

            size_t getRowCount();

            std::shared_ptr<arrow::Schema> schema();
        };

        class ParquetProvider : public DataFrameSpace::DataProvider {
            std::unique_ptr<parquet::arrow::FileReader> reader_;
            std::vector<size_t> rowGroupOffsets_;
        public:
            ParquetProvider(std::string path) {
                std::shared_ptr<arrow::io::ReadableFile> infile;
                PARQUET_ASSIGN_OR_THROW(
                        infile,
                        arrow::io::ReadableFile::Open(path,
                                                      arrow::default_memory_pool()));

                PARQUET_THROW_NOT_OK(
                        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader_));

                rowGroupOffsets_.resize(reader_->num_row_groups());
                size_t offset;
                for (int i = 0; i < rowGroupOffsets_.size(); i++) {
                    rowGroupOffsets_[i] = offset;
                    offset += reader_->parquet_reader()->metadata()->RowGroup(i)->num_rows();
                }
            }

            void visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor,
                       size_t providerOffset, RangeLength rowsCount,
                       const std::vector<int> &columIndex) const override {
                size_t pos = 0;
                auto schema = reader_->parquet_reader()->metadata()->schema();
                int currentRowGroupPos =
                        std::lower_bound(rowGroupOffsets_.begin(), rowGroupOffsets_.end(), providerOffset) -
                        rowGroupOffsets_.begin();
                int localOffset = providerOffset - rowGroupOffsets_[currentRowGroupPos];
                while (currentRowGroupPos < rowGroupOffsets_.size() &&
                       rowGroupOffsets_[currentRowGroupPos] < providerOffset + rowsCount) {
                    std::shared_ptr<::arrow::Table> tablePtr;
                    PARQUET_THROW_NOT_OK(reader_->ReadRowGroup(currentRowGroupPos, columIndex, &tablePtr));
                    RangeLength currentCount = std::min(rowsCount,
                                                        static_cast<RangeLength>(tablePtr->num_rows() - localOffset));
                    visitor(*tablePtr, localOffset, currentCount);
                    rowsCount -= currentCount;
                }
            }

            size_t numRows() const override {
                return reader_->parquet_reader()->metadata()->num_rows();
            }
        };

        /**
         * Creates a data frame that maps virtually to a parquet file
         * @param sourceTable - a unique pointer to an arrow table - since we expect to use it as an immutable data
         * source, we need to ensure there are no other active pointers reaching the table.
         * @return
         */
        MutableDataFrame dataFrameFromParquetFile(std::string filePath) {
            MockParquetProvider parquetProvider(filePath);
            auto numRows = parquetProvider.getRowCount();
            // Step 1. Associate arrow table data with the data frame space
            //Create a DataFrameSpace matching the incoming schema
            auto dataFrameSpace = std::make_shared<DataFrameSpace>(parquetProvider.schema());
            //Register the incoming provider with the data frame space and retrieve an associated pointer
            auto index = dataFrameSpace->registerDataProvider(parquetProvider.getDataProvider(), numRows);
            //(Steps 3,4 do not apply here)
            //Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            //Create the actual data frame by composing the index with the data frame space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(std::move(index), dataFrameSpace);
            //Creates a mutable data frame having this as the original value
            return MutableDataFrame(immutableDataFrame);
        }


        /**
         * Inserts data from an arrow Table into a mutable data frame (it basically interleaves newRows with dataFrame
         * at locations specified by newRowRangePositions)
         * @param dataFrame DataFrame to be modified
         * @param newRows An arrow table containing all the new rows to be inserted
         * @param newRowRangePositions List of <insertion point,row count> indicating where the respective rows need to
         * be inserted in the mutable table and the number of rows that need to be inserted at each point
         */
        static void insertAt(MutableDataFrame &dataFrame, std::shared_ptr<arrow::Table> newRows,
                      std::vector<std::pair<size_t, size_t>> newRowRangePositions) {
            auto numNewRows = newRows->num_rows();
            //Starts the mutation process with a dataframe snapshot in
            // order to ensure we are working against fixed data;
            auto dataFrameSnapShot = dataFrame.versionAndSnapshot();
            //* Step 1. Associate arrow table data with the data frame space
            //Retrieve the current data frame space
            auto dataFrameSpace = dataFrameSnapShot.second->getSpace();
            //Confirm the incoming table is a constant
            //Add the new rows to the data frame space
            auto newRowsIndex = dataFrameSpace->registerExternalData(std::shared_ptr<arrow::Table>(std::move(newRows)));
            //* Step 3. Create a result index by combining one or more indices
            //We create an index session and we combine the new row index with existing data frame index:
            const auto &currentIndex = dataFrameSnapShot.second->getIndex();
            IndexMutationSession indexMutationSession(*dataFrameSpace);
            size_t prevOriginalFrameOffset = 0;
            size_t currentInsertOffset = 0;
            for (const auto &positionAndLen :  newRowRangePositions) {
                size_t offsetInOriginalFrame = positionAndLen.first;
                if (offsetInOriginalFrame > prevOriginalFrameOffset) {
                    //Add row range from existing data frame
                    indexMutationSession.addSubIndex(currentIndex, prevOriginalFrameOffset,
                                                     offsetInOriginalFrame - prevOriginalFrameOffset);
                    prevOriginalFrameOffset = offsetInOriginalFrame;
                }
                //Add the indices corresponding to the new rows
                indexMutationSession.addSubIndex(newRowsIndex, currentInsertOffset, positionAndLen.second);
                currentInsertOffset += positionAndLen.second;
            }
            if (prevOriginalFrameOffset < currentIndex.size()) {
                //Add any remaining rows
                indexMutationSession.addSubIndex(currentIndex, prevOriginalFrameOffset,
                                                 currentIndex.size() - prevOriginalFrameOffset);
            }
            std::pair<Index, TranslationLog> indexAndDefragmentation = indexMutationSession.close();
            //* Step 4. Apply any defragmentation by copying data onto the new buffers
            dataFrameSpace->applyDefragmentation(indexAndDefragmentation.second);
            //* Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(std::move(indexAndDefragmentation.first),
                                                                           dataFrameSpace);
            //apply the new value to the input frame together with the expected version in order to avoid concurrent changes:
            //If the dataFrame has changed since snapshot method was called at the beginning of the function, then mutate
            //would fail because expectedVersion would no longer be dataFrameSnapShot.first
            dataFrame.mutate(dataFrameSnapShot.first + 1, std::move(immutableDataFrame));
        }

        /**
         * Modifies a given set of row ranges - this is very similar to insertAt, the main different being that rather
         * than inserting new rowRanges, we replace row at the old ranges with new ones
         * @param dataFrame DataFrame to be modified
         * @param newRows An arrow table containing all the new rows values for the rows to be modified
         * @param newRowRangePositions List of <insertion point,row count> indicating where the respective rows need to
         * be modifier in the mutable table and the number of rows that need to be altered at each point
         */
        static void updateAt(MutableDataFrame &dataFrame, std::shared_ptr<arrow::Table> newRows,
                      std::vector<std::pair<size_t, size_t>> newRowRangePositions) {
            auto numNewRows = newRows->num_rows();
            //Starts the mutation process with a dataframe snapshot in
            // order to ensure we are working against fixed data;
            auto dataFrameSnapShot = dataFrame.versionAndSnapshot();
            //* Step 1. Associate arrow table data with the data frame space
            //Retrieve the current data frame space
            auto dataFrameSpace = dataFrameSnapShot.second->getSpace();
            //TODO Make the incoming table a constant
            //Add the new rows to the data frame space
            auto newRowsIndex = dataFrameSpace->registerExternalData(std::shared_ptr<arrow::Table>(std::move(newRows)));
            //* Step 3. Create a result index by combining one or more indices
            //We create an index session and we combine the new row index with existing data frame index:
            const auto &currentIndex = dataFrameSnapShot.second->getIndex();
            IndexMutationSession indexMutationSession(*dataFrameSpace);
            size_t prevOriginalFrameOffset = 0;
            size_t currentInsertOffset = 0;
            for (const auto &positionAndLen :  newRowRangePositions) {
                size_t offsetInOriginalFrame = positionAndLen.first;
                if (offsetInOriginalFrame > prevOriginalFrameOffset) {
                    //Add row range from existing data frame
                    indexMutationSession.addSubIndex(currentIndex, prevOriginalFrameOffset,
                                                     offsetInOriginalFrame - prevOriginalFrameOffset);
                    prevOriginalFrameOffset = offsetInOriginalFrame;
                }
                //Add the indices corresponding to the new rows
                indexMutationSession.addSubIndex(newRowsIndex, currentInsertOffset, positionAndLen.second);
                //***********!!!!!!!!!!!!!!!!!!!!!!!!!!***********
                //This is the only difference from the insertAt function - we effectively skip the old rows in the new index:
                prevOriginalFrameOffset += positionAndLen.second;

            }
            if (prevOriginalFrameOffset < currentIndex.size()) {
                //Add any remaining rows
                indexMutationSession.addSubIndex(currentIndex, prevOriginalFrameOffset,
                                                 currentIndex.size() - prevOriginalFrameOffset);
            }
            std::pair<Index, TranslationLog> indexAndDefragmentation = indexMutationSession.close();
            //* Step 4. Apply any defragmentation by copying data onto the new buffers
            dataFrameSpace->applyDefragmentation(indexAndDefragmentation.second);
            //* Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(std::move(indexAndDefragmentation.first),
                                                                           dataFrameSpace);
            //apply the new value to the input frame together with the expected version in order to avoid concurrent changes:
            //If the dataFrame has changed since snapshot method was called at the beginning of the function, then mutate
            //would fail because expectedVersion would no longer be dataFrameSnapShot.first
            dataFrame.mutate(dataFrameSnapShot.first + 1, std::move(immutableDataFrame));
        }

        /**
         * Creates a new data frame containing a subset of the input data frame (for example the rows matching a filter condition)
         * @param src data frame we are filtering
         * @param rangesToKeep A list of positional ranges expressed as <offset,length>
         * @return a new DataFrame containing only the requested ranges
         */
        static std::shared_ptr<MutableDataFrame> filter(const MutableDataFrame &src, std::vector<std::pair<size_t, size_t>> rangesToKeep) {
            //Starts the mutation process with a dataframe snapshot in
            // order to ensure we are working against fixed data;
            auto dataFrameSnapShot = src.versionAndSnapshot();
            //Retrieve the current data frame space
            auto dataFrameSpace = dataFrameSnapShot.second->getSpace();
            //* Step 1. N/A since there is no new external data
            //* Step 2. N/A since there is no new index
            //* Step 3. Create a result index by combining one or more indices
            //We create an index session and combine the subIndices we need to keep
            auto &currentIndex = dataFrameSnapShot.second->getIndex();
            IndexMutationSession indexMutationSession(*dataFrameSpace);
            for (const auto &rangeToKeep :  rangesToKeep) {
                size_t offsetInOriginalFrame = rangeToKeep.first;
                indexMutationSession.addSubIndex(currentIndex, rangeToKeep.first, rangeToKeep.second);
            }
            std::pair<Index, TranslationLog> indexAndDefragmentation = indexMutationSession.close();
            //* Step 4. Apply any defragmentation by copying data onto the new buffers
            dataFrameSpace->applyDefragmentation(indexAndDefragmentation.second);
            //* Step 5. Create a new DataFrame by combining the new index with the expanded Data Frame Space
            auto immutableDataFrame = std::make_shared<ImmutableDataFrame>(std::move(indexAndDefragmentation.first),
                                                                           dataFrameSpace);
            //Create a new mutable data frame around
            return std::make_shared<MutableDataFrame>(std::move(immutableDataFrame));
        }


        void puttingItAllTogether() {
            //Creates a data frame that virtually maps to a parquet file while allowing for lazy data loading
            MutableDataFrame df = dataFrameFromParquetFile("/path/To/parquet/file");

            //Following a filter operation we decide we want every 1000th element from the parquet file:
            std::vector<std::pair<size_t, size_t>> filteredRanges;
            for (size_t i = 0; i < df.size(); i += 1000) {
                filteredRanges.emplace_back(i, 1);
            }
            //we filter the content of the given parquet table and since the resulting data is very sparse the mutation
            //mechanism would automatically defragment, effectively copying the targeted rows in a new contiguous region
            auto every1000th = filter(df, filteredRanges);

            //A second new filter indicate that we are interested in 3 subregions of the parquet data frame of size 10,000 each
            std::vector<std::pair<size_t, size_t>> filter2 = {{12300, 10000},
                                                              {32100, 10000},
                                                              {65300, 10000}};

            //We apply the new filter range, but this time the likely result would map directly to the virtual parquet region same as df
            auto threeRanges = filter(df, filter2);

            //A third filter, shows that we want the 1000 contiguous elements followed by every 10 elements from the subsequet 1000 elements range
            std::vector<std::pair<size_t, size_t>> filter3;
            //todo init filter3 accordingly
            //The defragmentation logic in this case would likely reuse some of the large blocks while copying the sparse data in a new compact region
            auto hybridDf = filter(df, filter3);

            //... after processing hybridDf, we decide that we need to update the values at row 100, 200-250,300-1300:
            //First we store all the desired new row values in a new arrow::Table
            std::unique_ptr<arrow::Table> newTable;
            //todo initialize newTable to have 1201 rows with the respective desired row values for ranges 100, 200-250,300-1300
            //Now we apply the changes to hybridDf - the new version of hybridDf is expected to
            // - continue to point to most* of the same arrow array as the previous one (i.e. most of the unchanged copied data is not)
            // - point to the arrays from newTable for most* of the new rows
            // - a small number of rows (O(<number of cuts>) ... here O(3)), may be relocated to a new contiguous region in order to
            // maintain the defragmentation contract
            updateAt(*hybridDf, std::move(newTable), {{100, 1},
                                                     {200, 50},
                                                     {300, 1300}});
            //Threading note: By design - updateAt is executed concurrently by multiple threads we guarantee hybridDf final
            //value is actually correct


            //Takeaways:
            //- I have shown how virtual and non-virtual data can coexist in a data frame
            //- I have shown how a new data-frame can be derived from an existing one by reusing most* of the underlying data
            //- I have proposed a copy-write-model that maximizes* the data reuse
            //- I have proposed a fragmentation management constraint that balance shallow copies with providing expectations
            // regarding data density
            //- I have provided mechanism that allows for concurrent readers and a single writer accessing the same data frame
            //- I have provided means for detecting concurrent modifications and maintaining data consistency during a
            //concurrent modification scenario - The envision data structures can potentially accommodate concurrent
            //modification occurring on different data frame region, but that is not covered by this proposal.
            //Disclaimers:
            //- There are implementation details behind the Index architecture not available here, but I am happy to go
            // for a deep dive on that topic and I hope to release some independently working code illustrating the mechanisms
            //- I don't cover the actual details of guaranteeing the immutability of arrow arrays subordinated to an
            //arrow::Table as I am probably not the most qualified person to do it.
            //- I am somewhat dyslexic so please excuse spelling mistakes and flag statements that don't make any sense,
            //I promise there was though and meaning behind them, it just got corrupted during the transcription process
        }
    };

    Index DataFrameSpace::registerExternalData(std::shared_ptr<arrow::Table> newRows) {
        auto refId = spaceProviderImpl_.mapBlock(newRows->num_rows());
        blocksMap_[refId.id()] = newRows;
        return Index(std::move(refId), newRows->num_rows());
    }

    Index DataFrameSpace::registerDataProvider(const std::shared_ptr<DataProvider> &provider, size_t rowsCount) {
        auto refId = spaceProviderImpl_.mapBlock(rowsCount);
        providersMap_[refId.id()] = provider;
        return Index(std::move(refId), rowsCount);
    }

    void DataFrameSpace::registerData(SpacePointer targetPointer, std::shared_ptr<arrow::Table> &&newBlock) {
        assert(!blocksMap_.contains(targetPointer));
        blocksMap_.emplace(targetPointer, std::move(newBlock));
    }

    void
    DataFrameSpace::visit(std::function<void(const arrow::Table &, size_t, uint32_t)> visitor, SpacePointer spaceOffset,
                          RangeLength rowsCount, const std::vector<int> &columns) const {
        std::map<size_t, std::shared_ptr<arrow::Table>>::const_iterator blocksIt =
                blocksMap_.upper_bound(spaceOffset);
        blocksIt--;
        std::map<size_t, std::shared_ptr<DataProvider>>::const_iterator providerIt = providersMap_.end();
        do {
            RangeLength currentLen;
            if (blocksIt->first + blocksIt->second->num_rows() <= spaceOffset) {
                if (providerIt == providersMap_.end()) {
                    providerIt = providersMap_.upper_bound(spaceOffset);
                    providerIt--;
                } else {
                    assert(providerIt->first == spaceOffset);
                }
                size_t localOffset = spaceOffset - providerIt->first;
                currentLen = std::min(static_cast<RangeLength>(providerIt->second->numRows() - localOffset),
                                      rowsCount);
                providerIt->second->visit(visitor, localOffset, currentLen, columns);
                providerIt++;
            } else {
                arrow::Table &table = *blocksIt->second;
                size_t localOffset = spaceOffset - blocksIt->first;
                currentLen = std::min(static_cast<RangeLength>(table.num_rows() - localOffset), rowsCount);
                if (columns.empty()) {
                    visitor(table, localOffset, currentLen);
                } else {
                    std::vector<std::shared_ptr<arrow::Field>> subFields(columns.size());
                    std::vector<std::shared_ptr<arrow::ChunkedArray>> subColumns(columns.size());
                    for (size_t i = 0; i < columns.size(); i++) {
                        subFields[i] = table.schema()->field(columns[i]);
                        subColumns[i] = table.column(columns[i]);
                    }
                    auto subSchema = std::make_shared<arrow::Schema>(subFields);
                    auto subTable = arrow::Table::Make(std::make_shared<arrow::Schema>(subFields), std::move(subColumns));
                    visitor(*subTable, localOffset, currentLen);
                }
                blocksIt++;
            }
            rowsCount -= currentLen;
            spaceOffset += currentLen;
        } while (rowsCount);
    }

    void IndexMutationSession::addSubIndex(const Index &index, size_t offset, size_t len) {
        builder_.addNode(index.impl_, offset, len);
    }

    std::pair<Index, TranslationLog> IndexMutationSession::close() {
        auto indexImpl = builder_.close();
        auto translations = sessionImpl_->close();
        TranslationLog result;
        result.blockTranslation.reserve(translations.size());
        for (const auto &translation : translations) {
            TranslationUnit currentUnit(*translation.targetPointer_);
            std::pair<SpacePointer, RangeLength> currentRange = {0, 0};
            for (size_t i = 0; i < SPACE_BLOCK_SIZE; i++) {
                if (translation.sourceRows_[i] == NULL_ENTRY) {
                    if (currentRange.second) {
                        currentUnit.sourceRows_.emplace_back(currentRange);
                        currentRange.second = 0;
                    }
                } else if (currentRange.second) {
                    if (translation.sourceRows_[i] == currentRange.first + currentRange.second) {
                        currentRange.second++;
                    } else {
                        currentUnit.sourceRows_.emplace_back(currentRange);
                        currentRange = {translation.sourceRows_[i], 1};
                    }
                } else {
                    currentRange = {translation.sourceRows_[i], 1};
                }
            }
            if (currentRange.second) {
                currentUnit.sourceRows_.emplace_back(currentRange);
            }
            result.blockTranslation.emplace_back(currentUnit);
        }
        return {Index(std::move(indexImpl)), std::move(result)};
    }

    void Index::forEach(std::function<void(SpacePointer, RangeLength)> visitor, size_t offset, size_t length) const {
        BuilderT::forEachLeaf([&](const LeafT &leaf, size_t localOffset, size_t currentLen) {
            visitor(leaf[localOffset], currentLen);
        }, impl_, offset, length);
    }

    arrow::Status PrettyPrint(const DataFrame &dataFrameSrc, const arrow::PrettyPrintOptions &options,
                       std::ostream *sink) {
        auto dataFrame = dataFrameSrc.snapshot();
        std::shared_ptr<arrow::Schema> &schema = dataFrame->getSpace()->schema();
        RETURN_NOT_OK(PrettyPrint(*schema, options, sink));
        (*sink) << "\n";
        (*sink) << "----\n";
        size_t rangesCount = 0;
        auto& index = dataFrame->getIndex();
        index.forEach([&](SpacePointer offset,RangeLength length) {
            rangesCount++;
        },0,index.size());
        (*sink) << "RangesCount = " << rangesCount << " RowsCount = " << index.size() <<std::endl;
        (*sink) << "Space Index: [";

        int pos = 0;
        index.forEach([&](SpacePointer offset,RangeLength length) {
            if (pos > 10 && pos < rangesCount-10) {
                return;
            }
            if (pos == 10) {
                (*sink) << " ... ";
                return;
            }
            if (pos) {
                (*sink)<<",";
            }
            (*sink)<<"["<<offset<<","<<(offset+length)<<")";
        },0,index.size());
        (*sink) << "]\n";
        arrow::PrettyPrintOptions column_options = options;
        column_options.indent += 2;
        for (int i = 0; i < schema->num_fields(); ++i) {
            for (int j = 0; j < options.indent; ++j) {
                (*sink) << " ";
            }
            (*sink) << schema->field(i)->name() << ":\n";
            dataFrame->visit([&](const arrow::Table &innerTable, size_t offset, uint32_t len) {
                auto status = PrettyPrint(*innerTable.column(0)->Slice(offset, len), column_options, sink);
                if (!status.ok()) {
                    throw std::runtime_error(status.message());
                }
            }, {i}, 0, dataFrame->size());
            (*sink) << "\n";
        }
        (*sink) << std::flush;
        return arrow::Status::OK();
    }

}
#endif //EXPERIMENTS_FRAMESPACEIMPL_H
