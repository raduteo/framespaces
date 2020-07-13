#ifndef EXPERIMENTS_FIXEDSIZE_ARRAY_ALLOCATOR_H
#define EXPERIMENTS_FIXEDSIZE_ARRAY_ALLOCATOR_H

#include "FixedSizeAllocator.h"
#include <type_traits>


template<class T,size_t Size>
class StdFixedSizeArrayAllocator {
    static_assert(std::is_trivially_destructible<T>::value);
    static_assert(std::is_trivially_copyable<T>::value);
public:
    // type definitions
    typedef T value_type;
    typedef T *pointer;
    typedef const T *const_pointer;
    typedef T &reference;
    typedef const T &const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    // rebind allocator to type U
    template<class U>
    struct rebind {
        typedef StdFixedAllocator<U> other;
    };

    static constexpr size_t normalizedSize(size_t size) {
        return ((size >> 3u) + ((size & 0x7u) ? 1u : 0)) << 3u;
    }

    static constexpr size_t normalizedSize() {
        return normalizedSize(sizeof(T[Size]));
    }
    using internalAllocator =  FixedSizeAllocator<normalizedSize()>;

    static StdFixedSizeArrayAllocator& oneAndOnly(){
        static StdFixedSizeArrayAllocator result;
        return result;
    }


    // return address of values
    pointer address(reference value) const {
        return &value;
    }

    const_pointer address(const_reference value) const {
        return &value;
    }

    /* constructors and destructor
     * - nothing to do because the allocator has no state
     */
    StdFixedSizeArrayAllocator() throw() {
    }

    StdFixedSizeArrayAllocator(const StdFixedSizeArrayAllocator &) throw() {
    }

    template<class U>
    StdFixedSizeArrayAllocator(const StdFixedAllocator<U> &) throw() {
    }

    ~StdFixedSizeArrayAllocator() throw() {
    }

    // return maximum number of elements that can be allocated
    size_type max_size() const throw() {
        return 1;
    }

    // allocate but don't initialize num elements of type T
    pointer allocate(size_type num, const void * = 0) {
        // print message and allocate memory with global new
        assert(num == 1);
        //std::cerr << "allocate " << num << " element(s)"
        //          << " of size " << sizeof(T) << std::endl;
        pointer ret = (pointer) (internalAllocator::oneAndOnly().alloc());
        //std::cerr << " allocated at: " << (void *) ret << std::endl;
        return ret;
    }

    // initialize elements of allocated storage p with value value
    void construct(pointer p, const T &value) {
        // initialize memory with placement new
        new((void *) p)T(value);
    }

    // destroy elements of initialized storage p
    void destroy(pointer p) {
        // destroy objects by calling their destructor
        p->~T();
    }

    // deallocate storage p of deleted elements
    void deallocate(pointer p, size_type num) {
        assert(num == 1);
        // print message and deallocate memory with global delete
        /*std::cerr << "deallocate " << num << " element(s)"
                  << " of size " << sizeof(T)
                  << " at: " << (void *) p << std::endl;*/
        internalAllocator::oneAndOnly().free((void *) p);
    }
};

// return that all specializations of this allocator are interchangeable
template<class T1, class T2,size_t SIZE1, size_t SIZE2>
bool operator==(const StdFixedSizeArrayAllocator<T1,SIZE1> &,
                const StdFixedSizeArrayAllocator<T2,SIZE2> &) throw() {
    return StdFixedSizeArrayAllocator<T1,SIZE1>::normalizedSize() == StdFixedSizeArrayAllocator<T2,SIZE2>::normalizedSize();
}

template<class T1, class T2,size_t SIZE1, size_t SIZE2>
bool operator!=(const StdFixedSizeArrayAllocator<T1,SIZE1> &,
                const StdFixedSizeArrayAllocator<T2,SIZE2> &) throw() {
    return  StdFixedSizeArrayAllocator<T1,SIZE1>::normalizedSize() != StdFixedSizeArrayAllocator<T2,SIZE2>::normalizedSize();
}

#endif //EXPERIMENTS_FIXEDSIZEALLOCATOR_H
