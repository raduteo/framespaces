#include <iostream>
#include <vector>
#include <chrono>

#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/type.h"
#include "arrow/array/builder_primitive.h"
#include "Builder.h"
#include "spaces/FrameSpace.h"
#include "arrow/compute/api_vector.h"


std::shared_ptr<arrow::Table>
buildSampleTable(size_t count, int64_t idOffset = 0, int64_t idStep = 1, double vOffset = 0, double vStep = 10.1) {
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    arrow::Int64Builder id_builder(pool);
    arrow::DoubleBuilder val_builder(pool);

    for (int i = 0; i < count; i++) {
        if (!id_builder.Append(idOffset + i * idStep).ok()) {
            throw std::logic_error("Unexpected error");
        }
        if (!val_builder.Append(vOffset + i * vStep).ok()) {
            throw std::logic_error("Unexpected error");
        }
    }
    std::shared_ptr<arrow::Array> values;
    std::shared_ptr<arrow::Array> ids;
    if (!val_builder.Finish(&values).ok() || !id_builder.Finish(&ids).ok()) {
        throw std::logic_error("Unexpected error");
    }

    return arrow::Table::Make(std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{
                                      arrow::field("id", id_builder.type()),
                                      arrow::field("value", val_builder.type())}),
                              {ids, values});
}

void frameTransformations(int initialSize, int compactInsertSize, int sparseInsertSize, size_t sparseInsertStep,
                          int filterSegments, int filterRetention) {
    auto df0 = framespaces::DataFrameOperationExamples::dataFrameFromTable(buildSampleTable(initialSize));
    framespaces::PrettyPrint(*df0, 0, &std::cout);

    framespaces::DataFrameOperationExamples::insertAt(*df0, buildSampleTable(compactInsertSize, 100, 1, 1000),
                                                      {{initialSize / 3, compactInsertSize}});
    framespaces::PrettyPrint(*df0, 0, &std::cout);

    std::vector<std::pair<size_t, size_t>> insertLocations;
    for (size_t i = 0; i < sparseInsertSize; i++) {
        insertLocations.emplace_back((i + 1) * sparseInsertStep, 1);
    }
    framespaces::DataFrameOperationExamples::insertAt(*df0, buildSampleTable(sparseInsertSize, 200, 1, 2000),
                                                      insertLocations);
    framespaces::PrettyPrint(*df0, 0, &std::cout);

    framespaces::DataFrameOperationExamples::updateAt(*df0,
                                                      buildSampleTable(sparseInsertSize * sparseInsertStep + 2, 11, 11,
                                                                       1.1, 1.1),
                                                      {{sparseInsertStep - 1,
                                                               sparseInsertSize * sparseInsertStep + 2}});
    framespaces::PrettyPrint(*df0, 0, &std::cout);

    std::vector<std::pair<size_t, size_t>> filterSteps;
    for (int i = 0;i < filterSegments;i++) {
        filterSteps.emplace_back( df0->size()*i/(filterSegments + 1),df0->size()*filterRetention/filterSegments/100);
    }
    auto df1 = framespaces::DataFrameOperationExamples::filter(*df0, filterSteps);
    framespaces::PrettyPrint(*df1, 0, &std::cout);
}


int main() {
    //Illustrates a sequence of data frame mutation and filtering operations with the intention of illustrating how blocks
    //of data are reused and respectively copy and compacted depending on the level of fragmentation in the result

    frameTransformations(5, 2, 2, 2, 3, 50);

    frameTransformations(5000, 10, 10, 2, 10, 10);

    return 0;
}

