
#ifndef EXPERIMENTS_COMPACT_H
#define EXPERIMENTS_COMPACT_H

#include <memory>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace framespaces {

    struct CompactionIndex {
        int32_t offset;
        uint16_t rangeLen;
        uint16_t arrayNumber;

        CompactionIndex() {};

        CompactionIndex(int32_t offset,
                        int32_t rangeLen,
                        int16_t arrayNumber) : offset(offset), rangeLen(rangeLen), arrayNumber(arrayNumber) {}

        };

        arrow::Result <std::shared_ptr<arrow::Array>>
        Compact(const arrow::ArrayVector &arrays, const std::vector<CompactionIndex> &compactionIndex,
                arrow::MemoryPool *pool = arrow::default_memory_pool());


    }  // namespace framespaces

#endif //EXPERIMENTS_COMPACT_H
