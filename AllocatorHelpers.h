#ifndef EXPERIMENTS_ALLOCATORHELPERS_H
#define EXPERIMENTS_ALLOCATORHELPERS_H

#include "FixedSizeAllocator.h"

template<class T, class Alloc>
struct DeleterForAllocator;

template<class T>
struct DeleterForAllocator<T, std::nullptr_t> {

    void operator()(T *__ptr) const noexcept {
        delete[] __ptr;
    }
};

template<class T, size_t Size>
struct DeleterForAllocator<T, StdFixedSizeArrayAllocator<T, Size>> {

inline static auto &allocator_ = StdFixedSizeArrayAllocator<T, Size>::oneAndOnly();

DeleterForAllocator() {}

void operator()(T *__ptr) {
    allocator_.deallocate(__ptr, 1);
}
};

template<class T>
struct DeleterForFixedAllocator {

    DeleterForFixedAllocator() {}

    void operator()(T *__ptr) {
        auto &allocator = StdFixedAllocator<T>::oneAndOnly();
        allocator.destroy(__ptr);
        allocator.deallocate(__ptr, 1);
    }
};


#endif //EXPERIMENTS_ALLOCATORHELPERS_H
