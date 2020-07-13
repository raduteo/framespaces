#ifndef EXPERIMENTS_FIXEDSIZEALLOCATOR_H
#define EXPERIMENTS_FIXEDSIZEALLOCATOR_H

#include <assert.h>

#define STEP_SIZE 2

#define STACK_LIMIT 1024
#define ALL_ONES_64 (~uint64_t(0))

#include <cstddef>
#include <array>
#include <deque>
#include <iostream>
#include <unordered_set>
#include <strings.h>

inline bool allocInfoPrint = false;

template<size_t SIZE>
class Block {
    static constexpr size_t realSizeInLong() {
        return (SIZE >> 3u) + 1u + ((SIZE & 0x7u) ? 1u : 0);
    }

    uint64_t mask = ALL_ONES_64;
    std::array<uint64_t, 64 * realSizeInLong()> data;

public:

    void *alloc(uint64_t prefix) {
        assert(mask);
        uint64_t firstEmptyBlock = ffsll(mask);
        assert(firstEmptyBlock);
        firstEmptyBlock--;
        static constexpr size_t step = realSizeInLong();
        data[firstEmptyBlock * step] = (prefix | firstEmptyBlock);
        mask &= ~(uint64_t(1) << firstEmptyBlock);
        return &data[firstEmptyBlock * step + 1];
    }

    void release(uint64_t id) {
        uint64_t bitToSet = uint64_t(1) << (id & ((1u << 6u) - 1u));
        assert((bitToSet & mask) == uint64_t(0));
        mask |= bitToSet;
    }

    bool isFull() {
        return !mask;
    }

    size_t allocatedCount() {
        return __builtin_popcountll(~mask);
    }

    void prefetch() {
        for (size_t pos = 0; pos < data.size(); pos++) {
            data[pos] = 0xfcfcfcfcfcfcfcfc;
        }
    }
};

template<class T, size_t Size>
class StdFixedSizeArrayAllocator;

template<size_t SIZE>
class FixedSizeAllocator {

    using BlockType = Block<SIZE>;

    std::deque<std::unique_ptr<Block<SIZE>>> blocks_ = std::deque<std::unique_ptr<Block<SIZE>>>(64);

    std::deque<void *> leafQueue_;
    std::array<std::deque<uint64_t>, 10> treeLevels_;
    std::array<size_t, 10> currentRoots_;
#ifdef  DEBUG
    std::unordered_set<void*> allocatedPointers_;
#endif

    FixedSizeAllocator() {
        for (auto &&deque : treeLevels_) {
            deque = {0};
        }
        currentRoots_.fill(0);
    }

public:

    static FixedSizeAllocator &oneAndOnly() {
        static FixedSizeAllocator result;
        return result;
    }

    void ensureSpace(uint8_t level, size_t pos) {
        if (treeLevels_[level].size() <= pos) {
            treeLevels_[level].resize(((pos / STEP_SIZE) + 1) * STEP_SIZE);
            if (level == 0) {
                size_t targetSize = treeLevels_[0].size() << 6;
                if (targetSize > blocks_.size()) {
                    blocks_.resize(targetSize);
                }
            }
        }
    }

    void *alloc() {
        //assert(allocatedPointers_.size() == allocatedCount());
        if (allocInfoPrint && SIZE != 32) {
            std::cout << "Alloc<" << SIZE << ">" << std::endl;
        }
        if (!leafQueue_.empty()) {
            auto result = leafQueue_.back();
            leafQueue_.pop_back();
            //std::cout<<size_t(result) <<" "<<SIZE<<std::endl;
#ifdef DEBUG
            if (allocatedPointers_.find(result) != allocatedPointers_.end()) {
                auto properCount = allocatedPointers_;
                auto measuredCount = allocatedCount();
                std::cout << "Busted" << std::endl;
            }
            allocatedPointers_.insert(result);
#endif
            //assert(allocatedPointers_.size() == allocatedCount());
            return result;
        }
        BlockType *targetBlock;
        uint64_t blockPos = getBlockPos();
        targetBlock = blockPos < blocks_.size() ? blocks_.at(blockPos).get(): nullptr;
        if (targetBlock == nullptr) {
#ifdef DEBUG
            auto allocated = allocatedPointers_.size();
#endif
//            std::cout << "Resetting allocator stack" << std::endl;
            //assert(allocatedPointers_.size() == allocatedCount());
            currentRoots_.fill(0);
            blockPos = getBlockPos();
            targetBlock =  blockPos < blocks_.size() ? blocks_.at(blockPos).get():nullptr;
            //assert(allocatedPointers_.size() == allocatedCount());
        }
        if (targetBlock == nullptr) {
            size_t targetSize = std::max(size_t(1),blocks_.size());
            while (blockPos >= targetSize) {
                targetSize *= 2;
            }
            blocks_.resize(targetSize);
            blocks_[blockPos] = std::make_unique<BlockType>();
            if (allocInfoPrint) {
                std::cout << "blocks_[blockPos] = std::make_unique<BlockType>();<" << SIZE << ">" << blockPos << " "
                          << allocatedCount() << std::endl;
            }
            targetBlock = blocks_[blockPos].get();
        }
        auto result = targetBlock->alloc(blockPos << 6u);
        if (targetBlock->isFull()) {
            uint64_t bitInParent = extractBitInParent(blockPos);
            ensureSpace(0, currentRoots_[0]);
            treeLevels_[0][currentRoots_[0]] |= bitInParent;
        }
        //std::cout<<size_t(result) <<" "<<SIZE<<" "<<blockPos<<std::endl;
#ifdef DEBUG
        if (allocatedPointers_.find(result) != allocatedPointers_.end()) {
            auto properCount = allocatedPointers_.size();
            auto measuredCount = allocatedCount();
            std::cout << "Busted" << std::endl;
        }
        allocatedPointers_.insert(result);
#endif
     /*   if (allocatedPointers_.size() != allocatedCount()) {
            auto properCount = allocatedPointers_.size();
            auto measuredCount = allocatedCount();
            std::cout << "Busted" << std::endl;
        }*/
        //assert(allocatedPointers_.size() == allocatedCount());
        return result;
    }

    uint64_t getBlockPos() {//go up the roots as long as they are full
        BlockType *targetBlock;
        uint8_t level = 0;
        while (treeLevels_[level][currentRoots_[level]] == ALL_ONES_64) {
            uint64_t bitInParent = extractBitInParent(currentRoots_[level]);
            level++;
            treeLevels_[level][currentRoots_[level]] |= bitInParent;
        }
        while (level > 0) {
            size_t freeChildPos =
                    ffsll(~treeLevels_[level][currentRoots_[level]]) - 1 + (currentRoots_[level] << 6);
            level--;
            currentRoots_[level] = freeChildPos;
            ensureSpace(level, freeChildPos);
        }
        size_t pos = ffsll(~treeLevels_[0][currentRoots_[0]]) - 1;
        uint64_t blockPos = (currentRoots_[0] << 6u) + pos;
        return blockPos;
    }

    void free(void *toRelease) {
        //std::cout<<"Free:"<<size_t(toRelease) <<" "<<SIZE<<std::endl;
        if (toRelease == nullptr) {
            std::cout << "Busted" << std::endl;
        }
#ifdef DEBUG
        if (allocatedPointers_.find(toRelease) == allocatedPointers_.end()) {
            std::cout << "Busted" << std::endl;
        }
        allocatedPointers_.erase(toRelease);
#endif
        leafQueue_.push_back(toRelease);
        while (leafQueue_.size() > STACK_LIMIT) {
            freeInternal();
        }
    }

    void freeInternal()  {
        uint64_t *memToFree = static_cast<uint64_t *>(leafQueue_.front());
        if (memToFree == nullptr) {
            std::cout << "Busted" << std::endl;
        }
        leafQueue_.pop_front();
        memToFree--;
        uint64_t id = *memToFree;
        auto &ownerBlock = blocks_[id >> 6];
        bool wasFull = ownerBlock->isFull();
        ownerBlock->release(id);
        uint8_t level = 0;
        id = id >> 6u;
        while (wasFull) {
            uint64_t bitPosInParent = ~extractBitInParent(id);
            id = id >> 6u;
            wasFull = treeLevels_[level][id] == ALL_ONES_64;
            treeLevels_[level][id] &= bitPosInParent;
            level++;
        }
    }

    size_t allocatedCount() {
        size_t result = 0;
        for (const auto &block : blocks_) {
            if (block != nullptr) {
                result += block->allocatedCount();
            }
        }
        return result - leafQueue_.size();
    }

    void reset() {
        while (!leafQueue_.empty()) {
            freeInternal();
        }

        for (const auto &block : blocks_) {
            if (block != nullptr) {
                if (block->allocatedCount() != 0) {
                    throw std::logic_error("Cannot reset an allocator while it contains allocated objects");
                }
            }
        }
        blocks_.clear();
        for (auto &&deque : treeLevels_) {
            deque = {0};
        }
        currentRoots_.fill(0);

    }

    void prefetch(size_t slotsCount,bool resetFirst = true) {
        if (resetFirst) {
            reset();
        }
        if (slotsCount > blocks_.size() << 6) {
            blocks_.resize((slotsCount >> 6) + 1);
        }
        for (auto &&block : blocks_) {
            if (block == nullptr) {
                block = std::make_unique<BlockType>();
                block->prefetch();
            }
        }
    }

    uint64_t extractBitInParent(uint64_t id) const { return uint64_t(1) << (id & ((1u << 6u) - 1u)); }
};

inline constexpr size_t normalizedSize(size_t size) {
    return ((size >> 3u) + ((size & 0x7u) ? 1u : 0)) << 3u;
}

template<class T>
inline constexpr size_t normalizedSize() {
    return normalizedSize(sizeof(T));
}


template<class T, size_t normSize = 0>
class StdFixedAllocator {
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

    using internalAllocator =  FixedSizeAllocator<normalizedSize<T>()>;

    static StdFixedAllocator &oneAndOnly() {
        static StdFixedAllocator result;
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
    StdFixedAllocator() throw() {
    }

    StdFixedAllocator(const StdFixedAllocator &) throw() {
    }

    template<class U>
    StdFixedAllocator(const StdFixedAllocator<U> &) throw() {
    }

    template<class U, size_t SIZE>
    StdFixedAllocator(const StdFixedSizeArrayAllocator<U, SIZE> &) throw() {
    }

    ~StdFixedAllocator() throw() {
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

    void construct(pointer p, T &&value) {
        // initialize memory with placement new
        new((void *) p)T(std::move(value));
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

    void prefetch(size_t slotsCount) {
        internalAllocator::oneAndOnly().prefetch(slotsCount);
    }

    void reset() {
        internalAllocator::oneAndOnly().reset();
    }

    size_t allocatedCount() {
        return internalAllocator::oneAndOnly().allocatedCount();
    }
};

// return that all specializations of this allocator are interchangeable
template<class T1, class T2, size_t SIZE1, size_t SIZE2>
bool operator==(const StdFixedAllocator<T1> &,
                const StdFixedAllocator<T2> &) throw() {
    return StdFixedAllocator<T1>::normalizedSize() == StdFixedAllocator<T2>::normalizedSize();
}

template<class T1, class T2>
bool operator!=(const StdFixedAllocator<T1> &,
                const StdFixedAllocator<T2> &) throw() {
    return StdFixedAllocator<T1>::normalizedSize() != StdFixedAllocator<T2>::normalizedSize();
}


#endif //EXPERIMENTS_FIXEDSIZEALLOCATOR_H
