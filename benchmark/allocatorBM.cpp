#include <benchmark/benchmark.h>
#include <iostream>
#include "../FixedSizeAllocator.h"

template<size_t SIZE>
static void BM_LinearAlloc(benchmark::State &state) {
    // Perform setup here
    auto &allocator = FixedSizeAllocator<SIZE>::oneAndOnly();
    bool touchData = state.range(1);
    uint64_t prefetchedAmount = std::min(state.max_iterations, (uint64_t(2) << 30) / SIZE);
    std::vector<char *> pointers;
    bool shouldPrefetch = state.range(0);
    if (shouldPrefetch) {
        allocator.prefetch(2*prefetchedAmount);
    }
    size_t count = 0;
    //size_t initialCount = allocator.allocatedCount();
    for (auto _ : state) {
        count++;
        if (count > prefetchedAmount) {
            state.PauseTiming();
            for (const auto &pointer : pointers) {
                allocator.free(pointer);
            }
            pointers.clear();
            count = 0;
            state.ResumeTiming();
        }
      //  size_t currentCount = allocator.allocatedCount();
        char *value = (char *) allocator.alloc();
        if (touchData) {
            for (int i = 0; i < SIZE; i++) {
                value[i] = 0x12;
            }
        }
        pointers.emplace_back(value);
        benchmark::DoNotOptimize(value);
    }
    for (const auto &pointer : pointers) {
        allocator.free(pointer);
    }
}

// Register the function as a benchmark
BENCHMARK_TEMPLATE(BM_LinearAlloc, 64)->Ranges({{0, 1},
                                                {0, 1}});
/*BENCHMARK_TEMPLATE(BM_LinearAlloc, 1024)->Ranges({{0, 1},
                                                  {0, 1}});
BENCHMARK_TEMPLATE(BM_LinearAlloc, 8192)->Ranges({{0, 1},
                                                  {0, 1}});*/

template<size_t SIZE>
static void BM_LinearNativeAlloc(benchmark::State &state) {
    // Perform setup here
    FixedSizeAllocator<SIZE> allocator;
    bool touchData = state.range(0);
    uint64_t prefetchedAmount = std::min(state.max_iterations, (uint64_t(4) << 30) / SIZE);
    std::vector<char *> pointers;
    size_t count = 0;
    for (auto _ : state) {
        count++;
        if (count > prefetchedAmount) {
            state.PauseTiming();
            for (const auto &pointer : pointers) {
                free(pointer);
            }
            pointers.clear();
            count = 0;
            state.ResumeTiming();
        }
        char *value = (char *) malloc(SIZE);
        if (touchData) {
            for (int i = 0; i < SIZE; i += 256) {
                value[i] = 0x12;
            }
        }
        pointers.emplace_back(value);
        benchmark::DoNotOptimize(value);
    }
}
/*
BENCHMARK_TEMPLATE(BM_LinearNativeAlloc, 64)->Range(0, 1);

BENCHMARK_TEMPLATE(BM_LinearNativeAlloc, 1024)->Range(0, 1);
BENCHMARK_TEMPLATE(BM_LinearNativeAlloc, 8192)->Range(0, 1);
*/
template<size_t SIZE>
static void BM_AllocWithReleases(benchmark::State &state) {
    // Perform setup here
    auto &allocator = FixedSizeAllocator<SIZE>::oneAndOnly();
    bool touchData = state.range(1);
    uint64_t prefetchedAmount = std::min(state.max_iterations, (uint64_t(4) << 30) / SIZE);
    std::vector<char *> pointers;
    bool shouldPrefetch = state.range(0);
    if (shouldPrefetch) {
        allocator.prefetch(prefetchedAmount);
    }
    size_t batchSize = state.range(2);
    size_t percentageFree = state.range(3);
    size_t freeSize = std::max(size_t(1), batchSize * percentageFree / 100);
    size_t count = 0;
    size_t allocCount = 0;
    size_t freeCount = freeSize;
    for (auto _ : state) {
        if (allocCount < batchSize) {
            char *value = (char *) allocator.alloc();
            if (touchData) {
                for (int i = 0; i < SIZE; i += 256) {
                    value[i] = 0x12;
                }
            }
            pointers.emplace_back(value);
            benchmark::DoNotOptimize(value);
            count++;
            allocCount++;
        } else {
            count--;
            allocator.free(pointers.back());
            pointers.pop_back();
            freeCount--;
            if (freeCount == 0) {
                allocCount = 0;
                freeCount = freeSize;
                if (count >= prefetchedAmount) {
                    state.PauseTiming();
                    for (const auto &pointer : pointers) {
                        allocator.free(pointer);
                    }
                    pointers.clear();
                    count = 0;
                    state.ResumeTiming();
                }
            }
        }
    }
}

BENCHMARK_TEMPLATE(BM_AllocWithReleases, 64)->Ranges({{0, 1},
                                                      {0, 1},
                                                      {2, 1024},
                                                      {1, 100}});

BENCHMARK_TEMPLATE(BM_AllocWithReleases, 1024)->Ranges({{0, 1},
                                                        {0, 1},
                                                        {2, 1024},
                                                        {1, 100}});
BENCHMARK_TEMPLATE(BM_AllocWithReleases, 8192)->Ranges({{0, 1},
                                                        {0, 1},
                                                        {2, 1024},
                                                        {1, 100}});


template<size_t SIZE>
static void BM_AllocWithRandomReleases(benchmark::State &state) {
    // Perform setup here
    FixedSizeAllocator<SIZE> allocator;
    bool touchData = state.range(1);
    uint64_t prefetchedAmount = std::min(state.max_iterations, (uint64_t(4) << 30) / SIZE);
    std::vector<char *> pointers;
    bool shouldPrefetch = state.range(0);
    if (shouldPrefetch) {
        allocator.prefetch(prefetchedAmount);
    }
    size_t batchSize = state.range(2);
    size_t percentageFree = state.range(3);
    size_t freeSize = std::max(size_t(1), batchSize * percentageFree / 100);
    size_t count = 0;
    size_t allocCount = 0;
    size_t freeCount = freeSize;

    std::vector<size_t> slotsToRemove;
    for (auto _ : state) {
        if (allocCount < batchSize) {
            char *value = (char *) allocator.alloc();
            if (touchData) {
                for (int i = 0; i < SIZE; i += 256) {
                    value[i] = 0x12;
                }
            }
            pointers.emplace_back(value);
            benchmark::DoNotOptimize(value);
            count++;
            allocCount++;
            if (allocCount == batchSize) {
                state.PauseTiming();
                slotsToRemove.clear();
                size_t prevValue = std::rand() % (pointers.size() / freeSize);
                for (int i = 0; i < freeSize; i++) {
                    slotsToRemove.emplace_back(prevValue);
                    prevValue = prevValue + std::rand() % ((pointers.size() - prevValue) / (freeSize - i)) + 1;
                }
                state.ResumeTiming();
            }
        } else {
            count--;
            freeCount--;
            allocator.free(pointers[slotsToRemove[freeCount]]);
            pointers[slotsToRemove[freeCount]] = nullptr;
            if (freeCount == 0) {
                allocCount = 0;
                freeCount = freeSize;
                state.PauseTiming();
                std::vector<char *> newPointers;
                for (const auto &pointer : pointers) {
                    if (pointer) {
                        newPointers.emplace_back(pointer);
                    }
                }
                pointers.swap(newPointers);
                state.ResumeTiming();
            }
        }
        if (count >= prefetchedAmount) {
            state.PauseTiming();
            for (const auto &pointer : pointers) {
                allocator.free(pointer);
            }
            pointers.clear();
            count = 0;
            state.ResumeTiming();
        }
    }
}


// Run the benchmark
BENCHMARK_MAIN();