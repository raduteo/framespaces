#include <benchmark/benchmark.h>
#include "../Leaf.h"
#include "../FixedSizeAllocator.h"

#define TOTAL_BYTE_SIZE (uint64_t(1)<<30)


template<size_t Size>
static void BM_Buffer_Create(benchmark::State &state) {
    using IBuffer = Leaf<int, Size>;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    FixedSizeAllocator<4 * Size> &allocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / Size);

    std::vector<IBuffer> buffers;

    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / Size;
    buffers.reserve(sizeCap);
    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (buffers.size() >= sizeCap) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            buffers.clear();
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            state.ResumeTiming();
        }
        //size_t currentCount = allocator.allocatedCount();
        //std::cout << "Current count " << currentCount << std::endl;
        auto buffer = IBuffer::createBuffer(nullptr);
        if (touchData) {
            for (int i = 0; i < Size; i++) {
                buffer[i] = 1234;
            }
        }
        buffers.emplace_back(std::move(buffer));
    }
    benchmark::DoNotOptimize(buffers);
    buffers.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}
// Register the function as a benchmark
#define APPLY_SIZE_TO_BM(BM) \
BENCHMARK_TEMPLATE(BM,16)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,64)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,256)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,1024)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,4096)->Range(0,1);

/*

 */
APPLY_SIZE_TO_BM(BM_Buffer_Create)

template<size_t Size>
static void BM_Buffer_Copy(benchmark::State &state) {
    using IBuffer = Leaf<int, Size>;
    bool makeConst = state.range(0);
    //Initializer<Size,Alloc>::run();
    FixedSizeAllocator<4 * Size> &allocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / Size);
    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / Size;

    std::vector<IBuffer> srcBuffers;
    srcBuffers.reserve(sizeCap / 2);
    for (int i = 0; i < sizeCap / 2; i++) {
        srcBuffers.emplace_back(IBuffer::createBuffer(nullptr));
    }
    if (makeConst) {
        for (auto &item : srcBuffers) {
            item.makeConst();
        }
    }

    std::vector<IBuffer> destBuffers;
    destBuffers.reserve(sizeCap / 2);

    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (destBuffers.size() >= sizeCap / 2) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            destBuffers.clear();
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            state.ResumeTiming();
        }

        destBuffers.push_back(srcBuffers[destBuffers.size()]);
    }
    benchmark::DoNotOptimize(destBuffers);
    destBuffers.clear();
    srcBuffers.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

APPLY_SIZE_TO_BM(BM_Buffer_Copy)

template<size_t Size>
static void BM_Buffer_Move(benchmark::State &state) {
    using IBuffer = Leaf<int, Size>;
    bool makeConst = state.range(0);
    //Initializer<Size,Alloc>::run();
    FixedSizeAllocator<4 * Size> &allocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / Size);
    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / Size;

    std::vector<IBuffer> srcBuffers;
    srcBuffers.reserve(sizeCap / 2);
    for (int i = 0; i < sizeCap / 2; i++) {
        srcBuffers.emplace_back(IBuffer::createBuffer(nullptr));
    }
    if (makeConst) {
        for (auto &item : srcBuffers) {
            item.makeConst();
        }
    }

    std::vector<IBuffer> destBuffers;
    destBuffers.reserve(sizeCap / 2);

    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (destBuffers.size() >= sizeCap / 2) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            destBuffers.clear();
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            srcBuffers.clear();
            for (int i = 0; i < sizeCap / 2; i++) {
                srcBuffers.emplace_back(IBuffer::createBuffer(nullptr));
            }
            if (makeConst) {
                for (auto &item : srcBuffers) {
                    item.makeConst();
                }
            }

            state.ResumeTiming();
        }

        destBuffers.push_back(std::move(srcBuffers[destBuffers.size()]));
    }
    benchmark::DoNotOptimize(destBuffers);
    destBuffers.clear();
    srcBuffers.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

APPLY_SIZE_TO_BM(BM_Buffer_Move)

template<size_t Size>
static void BM_Buffer_MakeConst(benchmark::State &state) {
    using IBuffer = Leaf<int, Size>;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    FixedSizeAllocator<4 * Size> &allocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / Size);

    std::vector<IBuffer> buffers;

    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / Size;
    buffers.reserve(sizeCap);
    for (int i = 0; i < sizeCap; i++) {
        buffers.emplace_back(IBuffer::createBuffer(nullptr));
    }
    int pos = 0;
    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (pos >= sizeCap) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            buffers.clear();
            for (int i = 0; i < sizeCap; i++) {
                buffers.emplace_back(IBuffer::createBuffer(nullptr));
            }
            pos = 0;
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            state.ResumeTiming();
        }
        //size_t currentCount = allocator.allocatedCount();
        //std::cout << "Current count " << currentCount << std::endl;

        buffers[pos++].makeConst();
    }
    benchmark::DoNotOptimize(buffers);
    buffers.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}
// Register the function as a benchmark
#define APPLY_SIZE_TO_BM_NO_RANGE(BM) \
BENCHMARK_TEMPLATE(BM,16);\
BENCHMARK_TEMPLATE(BM,64);\
BENCHMARK_TEMPLATE(BM,256);\
BENCHMARK_TEMPLATE(BM,1024);\
BENCHMARK_TEMPLATE(BM,4096);

/*

 */
APPLY_SIZE_TO_BM_NO_RANGE(BM_Buffer_MakeConst)

template<size_t Size>
static void BM_Buffer_MakeMutable(benchmark::State &state) {
    using IBuffer = Leaf<int, Size>;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    FixedSizeAllocator<4 * Size> &allocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / Size);

    std::vector<IBuffer> buffers;

    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / Size;
    buffers.reserve(sizeCap);
    for (int i = 0; i < sizeCap; i++) {
        auto buf = IBuffer::createBuffer(nullptr);
        buf.makeConst();
        buffers.emplace_back(std::move(buf));
    }
    int pos = 0;
    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (pos >= sizeCap) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            buffers.clear();
            for (int i = 0; i < sizeCap; i++) {
                auto buf = IBuffer::createBuffer(nullptr);
                buf.makeConst();
                buffers.emplace_back(std::move(buf));
            }
            pos = 0;
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            state.ResumeTiming();
        }
        //size_t currentCount = allocator.allocatedCount();
        //std::cout << "Current count " << currentCount << std::endl;

        buffers[pos++].mutate(nullptr);
    }
    benchmark::DoNotOptimize(buffers);
    buffers.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

APPLY_SIZE_TO_BM_NO_RANGE(BM_Buffer_MakeMutable)

template<size_t Size>
static void BM_Buffer_AddHalf(benchmark::State &state) {
    using IBuffer = Leaf<int, Size>;
    //Initializer<Size,Alloc>::run();
    FixedSizeAllocator<4 * Size> &allocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / Size);
    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / Size;

    std::vector<IBuffer> srcBuffers;
    srcBuffers.reserve(sizeCap / 2);
    std::vector<IBuffer> destBuffers;
    destBuffers.reserve(sizeCap / 2);

    int data[Size / 2];
    for (int i = 0; i < Size / 2; i++) {
        data[i] = i * 10;
    }
    for (int i = 0; i < sizeCap / 2; i++) {
        auto buf = IBuffer::createBuffer(nullptr);
        buf.add(data, Size/2);
        srcBuffers.emplace_back(std::move(buf));
        destBuffers.emplace_back(IBuffer::createBuffer(nullptr));
    }

    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    int pos = 0;
    for (auto _ : state) {
        if (pos >= sizeCap / 2) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            destBuffers.clear();
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            for (int i = 0; i < sizeCap / 2; i++) {
                destBuffers.emplace_back(IBuffer::createBuffer(nullptr));
            }
            pos = 0;
            state.ResumeTiming();

        }
        destBuffers[pos].add(srcBuffers[pos],0,Size/2);
        destBuffers[pos].add(srcBuffers[pos],0,Size/2);
        pos++;
    }
    benchmark::DoNotOptimize(destBuffers);
    destBuffers.clear();
    srcBuffers.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

APPLY_SIZE_TO_BM_NO_RANGE(BM_Buffer_AddHalf)