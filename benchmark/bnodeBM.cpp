#include <benchmark/benchmark.h>
#include "utilities.h"
#include "../ANode.h"
#include "../BNode.h"
#include "../FixedSizeAllocator.h"

#define TOTAL_BYTE_SIZE (uint64_t(1)<<30)


template<size_t MaxCount, size_t Size>
static void BM_BNode_Create(benchmark::State &state) {
    /*
     * Results: no touch - 50 (12.5 ns/child),54 (7 ns/child),88 (4.4 ns/child) ,132 (4.1 ns/child),325 (5 ns/child)
     * Children Add: - 30 (10 ns/child), 95 (14 ns/child), 175 (12 ns/child), 400 (13ns/child), 807 (13 ns/child)
     */
    using BNode = BNode<int, MaxCount, Size>;
    using BNodePtr = typename BNode::BNodePtr;
    using Allocator = typename BNode::Allocator;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    auto &allocator = Allocator::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / sizeof(BNode));

    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    buffAllocator.prefetch(16);

    FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly().prefetch(16);

    BNode bNode(1);
    using Leaf = Leaf<int, Size>;
    auto buffer = Leaf::createBuffer(nullptr);
    std::array<int, Size> sampleData;
    for (int i = 0; i < Size; i++) {
        sampleData[i] = 3 * i;
    }
    buffer.add(sampleData.data(), Size);
    bNode.addNode(Leaf::createBufferPtr(buffer));
    bNode.makeConst(true);

    auto constNode = BNode::makeConstFromPtr(Leaf::createBufferPtr(buffer));

    std::vector<BNodePtr> bnodes;

    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / sizeof(BNode);
    bnodes.reserve(sizeCap);
    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (bnodes.size() >= sizeCap) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            bnodes.clear();
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            state.ResumeTiming();
        }
        //size_t currentCount = allocator.allocatedCount();
        //std::cout << "Current count " << currentCount << std::endl;
        auto nodePtr = BNode::createNodePtr(bNode);
        if (touchData) {
            for (int i = 1; i < MaxCount; i++) {
                nodePtr->addNode(constNode);
            }
        }
        bnodes.emplace_back(std::move(nodePtr));
    }
    benchmark::DoNotOptimize(bnodes);
    bnodes.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

template<size_t MaxCount, size_t Size>
static void BM_BNode_CopyMutable(benchmark::State &state) {
    /*
     * Results: no touch (relative to const copy)- +95, +90, +75 ,+91,+92
     * Children Add: - 30 (10 ns/child), 95 (14 ns/child), 175 (12 ns/child), 400 (13ns/child), 807 (13 ns/child)
     */
    using BNode = BNode<int, MaxCount, Size>;
    using BNodePtr = typename BNode::BNodePtr;
    using Allocator = typename BNode::Allocator;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    auto &bufferObjectAllocator = FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly();
    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();

    // std::cout << "Initial Leaf Count " << buffAllocator.allocatedCount() << std::endl;
    // std::cout << "Initial Leaf Object Count " << bufferObjectAllocator.allocatedCount() << std::endl;

    size_t sizeCap = std::min(TOTAL_BYTE_SIZE / sizeof(BNode), TOTAL_BYTE_SIZE / 4 / Size);
    auto &allocator = Allocator::oneAndOnly();
    allocator.prefetch(sizeCap * 2);

    buffAllocator.prefetch(sizeCap * 2);

    bufferObjectAllocator.prefetch(sizeCap * 2);

    BNode bNode(1);
    using Leaf = Leaf<int, Size>;
    auto buffer = Leaf::createBuffer(nullptr);
    std::array<int, Size> sampleData;
    for (int i = 0; i < Size; i++) {
        sampleData[i] = 3 * i;
    }
    buffer.add(sampleData.data(), Size);
    bNode.addNode(Leaf::createBufferPtr(buffer));
    //bNode.makeConst(true);

    auto constNode = BNode::makeConstFromPtr(Leaf::createBufferPtr(buffer));

    std::vector<BNodePtr> bnodes;


    bnodes.reserve(sizeCap);
    size_t initialCount = allocator.allocatedCount();
    //
    for (auto _ : state) {
        if (bnodes.size() >= sizeCap) {
            state.PauseTiming();
            /*std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            std::cout << "Leaf Count before clear " << buffAllocator.allocatedCount() << std::endl;
            std::cout << "Leaf Object Count before clear " << bufferObjectAllocator.allocatedCount() << std::endl;*/
            bnodes.clear();
            /* std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
             std::cout << "Leaf count after clear " << buffAllocator.allocatedCount() << std::endl;
             std::cout << "Leaf Object Count after clear " << bufferObjectAllocator.allocatedCount() << std::endl;*/
            state.ResumeTiming();
        }
        //size_t currentCount = allocator.allocatedCount();
        //std::cout << "Current count " << currentCount << std::endl;
        auto nodePtr = BNode::createNodePtr(bNode);
        if (touchData) {
            for (int i = 1; i < MaxCount; i++) {
                nodePtr->addNode(constNode);
            }
        }
        bnodes.emplace_back(std::move(nodePtr));
    }
    benchmark::DoNotOptimize(bnodes);
    bnodes.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

template<size_t MaxCount, size_t Size>
static void BM_BNode_MoveMutable(benchmark::State &state) {
    using BNode = BNode<int, MaxCount, Size>;
    using BNodePtr = typename BNode::BNodePtr;
    using Allocator = typename BNode::Allocator;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    auto &bufferObjectAllocator = FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly();
    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();

    //std::cout << "Initial Leaf Count " << buffAllocator.allocatedCount() << std::endl;
    //std::cout << "Initial Leaf Object Count " << bufferObjectAllocator.allocatedCount() << std::endl;

    size_t sizeCap = std::min(state.max_iterations,
                              std::min(TOTAL_BYTE_SIZE / sizeof(BNode), TOTAL_BYTE_SIZE / 4 / Size / MaxCount));
    auto &allocator = Allocator::oneAndOnly();
    allocator.prefetch(sizeCap * 2);

    buffAllocator.prefetch(sizeCap * 4 * MaxCount);

    bufferObjectAllocator.prefetch(sizeCap * 4 * MaxCount);
    {
        BNode bNode(1);
        using Leaf = Leaf<int, Size>;
        auto buffer = Leaf::createBuffer(nullptr);
        std::array<int, Size> sampleData;
        for (int i = 0; i < Size; i++) {
            sampleData[i] = 3 * i;
        }
        buffer.add(sampleData.data(), Size);
        if (touchData) {
            for (int i = 0; i < MaxCount; i++) {
                bNode.addNode(Leaf::createBufferPtr(buffer));
            }
        }

        //bNode.makeConst(true);


        std::vector<BNodePtr> bSrc;
        for (size_t i = 0; i < sizeCap; i++) {
            bSrc.push_back(BNode::createNodePtr(bNode));
        }
        std::vector<BNodePtr> bnodes;


        bnodes.reserve(sizeCap);
        size_t initialCount = allocator.allocatedCount();
        //
        for (auto _ : state) {
            if (bnodes.size() >= sizeCap) {
                state.PauseTiming();
                /*std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
                std::cout << "Leaf Count before clear " << buffAllocator.allocatedCount() << std::endl;
                std::cout << "Leaf Object Count before clear " << bufferObjectAllocator.allocatedCount() << std::endl;*/
                bSrc.swap(bnodes);
                bnodes.clear();
                /* std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
                 std::cout << "Leaf count after clear " << buffAllocator.allocatedCount() << std::endl;
                 std::cout << "Leaf Object Count after clear " << bufferObjectAllocator.allocatedCount() << std::endl;*/
                state.ResumeTiming();
            }
            //size_t currentCount = allocator.allocatedCount();
            //std::cout << "Current count " << currentCount << std::endl;
            auto nodePtr = BNode::createNodePtr(std::move(*bSrc[bnodes.size()]));
            bnodes.emplace_back(std::move(nodePtr));
        }
        benchmark::DoNotOptimize(bnodes);
        bnodes.clear();
    }
    bufferObjectAllocator.reset();
    buffAllocator.reset();
    allocator.reset();

    // std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

#define APPLY_SIZE_TO_BM(BM, COUNT) \
BENCHMARK_TEMPLATE(BM,COUNT,16)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,COUNT,64)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,COUNT,256)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,COUNT,1024)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,COUNT,4096)->Range(0,1)


#define APPLY_SIZE_AND_COUNT_TO_BM(BM) \
APPLY_SIZE_TO_BM(BM,4);\
APPLY_SIZE_TO_BM(BM,8);\
APPLY_SIZE_TO_BM(BM,16);\
APPLY_SIZE_TO_BM(BM,32);\
APPLY_SIZE_TO_BM(BM,64)

APPLY_SIZE_AND_COUNT_TO_BM(BM_BNode_Create);
APPLY_SIZE_AND_COUNT_TO_BM(BM_BNode_CopyMutable);
APPLY_SIZE_AND_COUNT_TO_BM(BM_BNode_MoveMutable);


template<size_t MaxCount, size_t Size>
static void BM_BNode_MakeConst(benchmark::State &state) {
    allocInfoPrint = false;
    using BNode = BNode<int, MaxCount, Size>;
    using BNodePtr = typename BNode::BNodePtr;
    using Allocator = typename BNode::Allocator;
    //Initializer<Size,Alloc>::run();{
    auto &bufferObjectAllocator = FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly();
    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    auto &allocator = Allocator::oneAndOnly();
    auto &sharedPtrAllocator = FixedSizeAllocator<32>::oneAndOnly();
    {

        /*   std::cout << "Initial Leaf Count " << buffAllocator.allocatedCount() << std::endl;
           std::cout << "Initial Leaf Object Count " << bufferObjectAllocator.allocatedCount() << std::endl;*/
        size_t sizeCap = std::min(state.max_iterations,
                                  std::min(TOTAL_BYTE_SIZE / sizeof(BNode), TOTAL_BYTE_SIZE / 4 / Size / MaxCount));
        allocator.prefetch(sizeCap * 2);

        buffAllocator.prefetch(sizeCap * 2 * MaxCount);
        sharedPtrAllocator.prefetch(sizeCap * 3 * MaxCount);


        bufferObjectAllocator.prefetch(sizeCap * 2 * MaxCount);

        BNode bNode(1);
        using Leaf = Leaf<int, Size>;
        auto buffer = Leaf::createBuffer(nullptr);
        std::array<int, Size> sampleData;
        for (int i = 0; i < Size; i++) {
            sampleData[i] = 3 * i;
        }
        buffer.add(sampleData.data(), Size);

        for (int i = 0; i < MaxCount; i++) {
            bNode.addNode(Leaf::createBufferPtr(buffer));
        }

        std::vector<BNodePtr> bSrc;
        for (size_t i = 0; i < sizeCap; i++) {
            bSrc.push_back(BNode::createNodePtr(bNode));
        }

        size_t initialCount = allocator.allocatedCount();
        size_t pos = 0;
        allocInfoPrint = true;
        //std::cout<<"***********Profile start***********" << sizeCap <<std::endl;
        for (auto _ : state) {
            if (pos >= sizeCap) {
                state.PauseTiming();
                allocInfoPrint = false;
                /*  std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
                  std::cout << "Leaf Count before clear " << buffAllocator.allocatedCount() << std::endl;
                  std::cout << "Leaf Object Count before clear " << bufferObjectAllocator.allocatedCount() << std::endl;
                  std::cout << "SharedPtr Count before clear "
                            << sharedPtrAllocator.allocatedCount() << std::endl;*/
                bSrc.clear();
                /*   std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
                   std::cout << "Leaf count after clear " << buffAllocator.allocatedCount() << std::endl;
                   std::cout << "Leaf Object Count after clear " << bufferObjectAllocator.allocatedCount() << std::endl;
                   std::cout << "SharedPtr Count after clear "
                             << sharedPtrAllocator.allocatedCount() << std::endl;*/
                for (size_t i = 0; i < sizeCap; i++) {
                    bSrc.push_back(BNode::createNodePtr(bNode));
                }
                pos = 0;
                // allocInfoPrint = true;
                state.ResumeTiming();
            }
            //size_t currentCount = allocator.allocatedCount();
            //std::cout << "Current count " << currentCount << std::endl;
            bSrc[pos++]->makeConst();
        }
        allocInfoPrint = false;
        // std::cout<<"***********Profile end***********"<<std::endl;

        benchmark::DoNotOptimize(bSrc);
        bSrc.clear();
        //todo clear the allocators at the end
        /*  std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
          std::cout << "SharedPtr Count at the end "
                    << sharedPtrAllocator.allocatedCount() << std::endl;*/
    }
    bufferObjectAllocator.reset();
    buffAllocator.reset();
    allocator.reset();
    sharedPtrAllocator.reset();

}

#define APPLY_SIZE_TO_BM_NO_RANGE(BM, COUNT) \
BENCHMARK_TEMPLATE(BM,COUNT,16);\
BENCHMARK_TEMPLATE(BM,COUNT,64);\
BENCHMARK_TEMPLATE(BM,COUNT,256);\
BENCHMARK_TEMPLATE(BM,COUNT,1024);\
BENCHMARK_TEMPLATE(BM,COUNT,4096)


#define APPLY_SIZE_AND_COUNT_TO_BM_NR(BM) \
APPLY_SIZE_TO_BM_NO_RANGE(BM,4);\
APPLY_SIZE_TO_BM_NO_RANGE(BM,8);\
APPLY_SIZE_TO_BM_NO_RANGE(BM,16);\
APPLY_SIZE_TO_BM_NO_RANGE(BM,32);\
APPLY_SIZE_TO_BM_NO_RANGE(BM,64)


APPLY_SIZE_AND_COUNT_TO_BM_NR(BM_BNode_MakeConst);


template<class T, size_t MAX_CHILDREN_COUNT, size_t BufferSize>
BNode<T, MAX_CHILDREN_COUNT, BufferSize> buildTree(int level, int childrenCount, int bufferSize) {
    using BNode = BNode<T, MAX_CHILDREN_COUNT, BufferSize>;
    using Leaf = Leaf<T, BufferSize>;
    BNode result(level);
    if (level == 1) {
        const static std::array<T, BufferSize> mockBuffer{};
        auto buffer = Leaf::createBuffer(nullptr);
        buffer.add(mockBuffer.data(), bufferSize);
        for (size_t i = 0; i < childrenCount; i++) {
            result.addNode(Leaf::createBufferPtr(buffer));
        }
    } else {
        for (size_t i = 0; i < childrenCount; i++) {
            result.addNode(BNode::createNodePtr(
                    buildTree<T, MAX_CHILDREN_COUNT, BufferSize>(level - 1, childrenCount, bufferSize)));
        }
    }
    return result;
}

template<class Node, class T>
void setValues(Node &dest, T *helperBuffer, size_t offset, size_t length, T initialValue, T step, int bufferSize) {
    T rollingValue = initialValue;
    for (; offset < length; offset += bufferSize) {
        for (size_t valuePos = 0; valuePos < bufferSize; valuePos++, rollingValue += step) {
            helperBuffer[valuePos % bufferSize] = rollingValue;
        }
        dest.setValues(helperBuffer, offset, bufferSize);
    }
}

template<class Node, class T>
void setValuesMock(Node &dest, T *helperBuffer, size_t offset, size_t length, int bufferSize) {
    size_t valuePos = 0;
    /*   for (T rollingValue = initialValue; valuePos < bufferSize; valuePos++, rollingValue += step) {
           helperBuffer[valuePos % bufferSize] = rollingValue;
       }*/
    for (; offset < length; offset += bufferSize) {
        dest.setValues(helperBuffer, offset, bufferSize);
    }
}


template<size_t MaxCount, size_t Size>
static void BM_BNode_FillData(benchmark::State &state) {
    allocInfoPrint = false;
    using BNode = BNode<int, MaxCount, Size>;
    using BNodePtr = typename BNode::BNodePtr;
    using Allocator = typename BNode::Allocator;
    //Initializer<Size,Alloc>::run();{
    auto &bufferObjectAllocator = FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly();
    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    auto &allocator = Allocator::oneAndOnly();
    size_t bufferSize = state.range(0);
    size_t totalSize = state.range(1);
    size_t height = heightForSize(totalSize, MaxCount, Size);
    totalSize = sizeForHeight(height, MaxCount, Size);
    assert(totalSize >= state.range(1));
    {

        /*   std::cout << "Initial Leaf Count " << buffAllocator.allocatedCount() << std::endl;
           std::cout << "Initial Leaf Object Count " << bufferObjectAllocator.allocatedCount() << std::endl;*/

        size_t sizeCap = std::min(state.max_iterations, TOTAL_BYTE_SIZE / 8 / totalSize);
        if (!sizeCap) {
            sizeCap++;
        }
        size_t bufferCount = (totalSize / Size + 100) * (sizeCap + 1);
        buffAllocator.prefetch(bufferCount);
        bufferObjectAllocator.prefetch(bufferCount);
        allocator.prefetch(bufferCount * 2);


        auto bNode = buildTree<int, MaxCount, Size>(heightForSize(totalSize, MaxCount, Size) - 1, MaxCount, Size);
        int *helperBuffer = new int[1000];
        setValues(bNode, helperBuffer, 0, bNode.size(), 11, 22, 1000);
        delete[] helperBuffer;
        std::vector<BNodePtr> bSrc;
        for (size_t i = 0; i < sizeCap; i++) {
            bSrc.push_back(BNode::createNodePtr(bNode));
        }

        int *readBuffer = new int[bufferSize];
        size_t initialCount = allocator.allocatedCount();
        size_t pos = 0;
        //std::cout<<"***********Profile start***********" << sizeCap <<std::endl;
        for (auto _ : state) {
            if (pos >= sizeCap) {
                pos = 0;
            }
            size_t offset = 0;
            int startValue = 11;
            do {
                size_t readCount = bSrc[pos]->fillBuffer(readBuffer, offset, bufferSize);
#ifdef DEBUG
                for (int i = 0; i < readCount;i++) {
                    assert(readBuffer[i] == startValue);
                    startValue += 22;
                }
#endif
                benchmark::DoNotOptimize(readBuffer);
                offset += readCount;
            } while (offset < bSrc[pos]->size());
            pos++;
        }
        allocInfoPrint = false;
        // std::cout<<"***********Profile end***********"<<std::endl;

        benchmark::DoNotOptimize(bSrc);
        bSrc.clear();
        state.SetBytesProcessed(state.iterations() * totalSize * sizeof(int));
        //todo clear the allocators at the end
        /*  std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
          std::cout << "SharedPtr Count at the end "
                    << sharedPtrAllocator.allocatedCount() << std::endl;*/
    }
    bufferObjectAllocator.reset();
    buffAllocator.reset();
    allocator.reset();
}

template<size_t MaxCount, size_t Size>
static void BM_BNode_SetData(benchmark::State &state) {
    allocInfoPrint = false;
    using BNode = BNode<int, MaxCount, Size>;
    using BNodePtr = typename BNode::BNodePtr;
    using Allocator = typename BNode::Allocator;
    //Initializer<Size,Alloc>::run();{
    auto &bufferObjectAllocator = FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly();
    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    auto &allocator = Allocator::oneAndOnly();
    size_t bufferSize = state.range(0);
    size_t totalSize = state.range(1);
    size_t height = heightForSize(totalSize, MaxCount, Size);
    totalSize = sizeForHeight(height, MaxCount, Size);
    assert(totalSize >= state.range(1));
    {

        /*   std::cout << "Initial Leaf Count " << buffAllocator.allocatedCount() << std::endl;
           std::cout << "Initial Leaf Object Count " << bufferObjectAllocator.allocatedCount() << std::endl;*/

        size_t sizeCap = std::min(state.max_iterations, TOTAL_BYTE_SIZE / 8 / totalSize);
        size_t bufferCount = (totalSize / Size + 100) * (sizeCap + 1);
        buffAllocator.prefetch(bufferCount);
        bufferObjectAllocator.prefetch(bufferCount);
        allocator.prefetch(bufferCount * 2);


        auto bNode = buildTree<int, MaxCount, Size>(heightForSize(totalSize, MaxCount, Size) - 1, MaxCount, Size);
        std::vector<BNodePtr> bSrc;
        for (size_t i = 0; i < sizeCap; i++) {
            bSrc.push_back(BNode::createNodePtr(bNode));
        }

        int *readBuffer = new int[bufferSize];
        size_t initialCount = allocator.allocatedCount();
        size_t pos = 0;
        //std::cout<<"***********Profile start***********" << sizeCap <<std::endl;
        int *helperBuffer = new int[bufferSize];
        for (int valuePos = 0,rollingValue = 11; valuePos < bufferSize; valuePos++, rollingValue += 33) {
            helperBuffer[valuePos % bufferSize] = rollingValue;
        }
        for (auto _ : state) {
            if (pos >= sizeCap) {
                pos = 0;
            }
            setValuesMock(bNode, helperBuffer, 0, bNode.size(), bufferSize);
            benchmark::DoNotOptimize(&bNode);
            pos++;
        }
        delete[] helperBuffer;
        allocInfoPrint = false;
        // std::cout<<"***********Profile end***********"<<std::endl;

        benchmark::DoNotOptimize(bSrc);
        bSrc.clear();
        state.SetBytesProcessed(state.iterations() * totalSize * sizeof(int));
        //todo clear the allocators at the end
        /*  std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
          std::cout << "SharedPtr Count at the end "
                    << sharedPtrAllocator.allocatedCount() << std::endl;*/
    }
    bufferObjectAllocator.reset();
    buffAllocator.reset();
    allocator.reset();
}

#define APPLY_SIZE_TO_BM_VAR_STEP(BM, COUNT) \
BENCHMARK_TEMPLATE(BM,COUNT,16)->Ranges({{16,1024},{1024,1024*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,64)->Ranges({{64,4096},{4096,4096*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,256)->Ranges({{256,16396},{16396,16396*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,1024)->Ranges({{1024,1024*64},{1024*64,1024*64*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,4096)->Ranges({{4096,4096*64},{4096*64,4096*64*1024}})


#define APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP(BM) \
APPLY_SIZE_TO_BM_VAR_STEP(BM,4);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,8);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,16);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,32);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,64)

APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP(BM_BNode_FillData);
APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP(BM_BNode_SetData);

//TODO - create a bunch of mutable objects and then make them const - expand to multi levels
//TODO - run a setValues and a fill for the same big objects