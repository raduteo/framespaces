#ifndef EXPERIMENTS_BUILDERFWD_H
#define EXPERIMENTS_BUILDERFWD_H
#include <cstddef>
#include "ArrayAdapterFwd.h"

template<class T, size_t MAX_COUNT, size_t SIZE,template<class, size_t> class ADAPTER = ArrayAdapter>
class Builder;

#endif //EXPERIMENTS_BUILDERFWD_H
