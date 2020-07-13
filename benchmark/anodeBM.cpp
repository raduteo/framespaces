#include <benchmark/benchmark.h>
#include "utilities.h"
#include "../ANode.h"
#include "../BNode.h"
#include "../FixedSizeAllocator.h"

#define TOTAL_BYTE_SIZE (uint64_t(1)<<28)


template<size_t MaxCount, size_t Size>
static void BM_ANode_Create(benchmark::State &state) {//TODO - implement/Same principles
    /*
     * Results: no touch - 40 (10 ns/child),50 (6.5 ns/child),88 (4.4 ns/child) ,132 (4.1 ns/child),325 (5 ns/child)
     * Children Add: - 33 (11 ns/child), 85 (12 ns/child), 190 (13 ns/child), 470 (15 ns/child), 1200 (19 ns/child)
     */
    using ANode = ANode<int, MaxCount, Size>;
    using ANodePtr = typename ANode::ANodePtr;
    using Allocator = typename ANode::Allocator;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    auto &allocator = Allocator::oneAndOnly();
    allocator.prefetch(TOTAL_BYTE_SIZE / 2 / sizeof(ANode));

    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();
    buffAllocator.prefetch(16);

    FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly().prefetch(16);
    FixedSizeAllocator<32>::oneAndOnly().prefetch(10);

    using Leaf = Leaf<int, Size>;
    auto buffer = Leaf::createBuffer(nullptr);
    std::array<int, Size> sampleData;
    for (int i = 0; i < Size; i++) {
        sampleData[i] = 3 * i;
    }
    buffer.add(sampleData.data(), Size);
    auto pcBuffer = BNode<int, MaxCount, Size>::makeConstFromPtr(Leaf::createBufferPtr(buffer));
    auto constNode = BNode<int, MaxCount, Size>::makeConstFromPtr(Leaf::createBufferPtr(buffer));

    ANode aNode(pcBuffer);

    std::vector<ANodePtr> anodes;

    size_t sizeCap = TOTAL_BYTE_SIZE / 4 / sizeof(ANode);
    anodes.reserve(sizeCap);
    size_t initialCount = allocator.allocatedCount();
    //std::cout << "Initial Count " << allocator.allocatedCount() << std::endl;
    for (auto _ : state) {
        if (anodes.size() >= sizeCap) {
            state.PauseTiming();
            //std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
            anodes.clear();
            //std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
            state.ResumeTiming();
        }
        //size_t currentCount = allocator.allocatedCount();
        //std::cout << "Current count " << currentCount << std::endl;
        auto nodePtr = ANode::createNodePtr(aNode);
        if (touchData) {
            for (int i = 1; i < MaxCount; i++) {
                nodePtr->addNode(constNode, 2, Size * 2 / MaxCount);
            }
        }
        anodes.emplace_back(std::move(nodePtr));
    }
    benchmark::DoNotOptimize(anodes);
    anodes.clear();
    //std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
}

template<size_t MaxCount, size_t Size>
static void BM_ANode_Move(benchmark::State &state) {
    /*
     * Results: no touch (once child only) - 50-100 (12-25 ns/child),80 (10 ns/child),100-140 (6-9 ns/child) ,140-200 (4.5-6 ns/child),225-325 (3.5-5 ns/child)
     * Children Add (actually the children count): - 9-15 (3-5 ns/child), 15-30 (2-4 ns/child), 60-80 (4-5 ns/child), 200-260 (6.5-8 ns/child), 425-664 (6.5-10.5 ns/child)
     */
    using ANode = ANode<int, MaxCount, Size>;
    using ANodePtr = typename ANode::ANodePtr;
    using Allocator = typename ANode::Allocator;
    bool touchData = state.range(0);
    //Initializer<Size,Alloc>::run();
    auto &bufferObjectAllocator = FixedSizeAllocator<sizeof(Leaf<int, Size>)>::oneAndOnly();
    FixedSizeAllocator<4 * Size> &buffAllocator = FixedSizeAllocator<4 * Size>::oneAndOnly();

    //std::cout << "Initial Leaf Count " << buffAllocator.allocatedCount() << std::endl;
    //std::cout << "Initial Leaf Object Count " << bufferObjectAllocator.allocatedCount() << std::endl;

    size_t sizeCap = std::max(uint64_t(MaxCount*Size*2),std::min(state.max_iterations,TOTAL_BYTE_SIZE / (sizeof(ANode) + MaxCount*32)/2));
    auto &allocator = Allocator::oneAndOnly();
    auto &sharedPtrAllocator = FixedSizeAllocator<32>::oneAndOnly();
    allocator.prefetch(sizeCap * 2);

    buffAllocator.prefetch(MaxCount * 16 );

    bufferObjectAllocator.prefetch(MaxCount * 16);
    sharedPtrAllocator.prefetch(sizeCap*MaxCount*2);

    {
        using Leaf = Leaf<int, Size>;
        auto buffer = Leaf::createBuffer(nullptr);
        std::array<int, Size> sampleData;
        for (int i = 0; i < Size; i++) {
            sampleData[i] = 3 * i;
        }
        buffer.add(sampleData.data(), Size);
        buffer.makeConst();
        ANode aNode(Leaf::createBufferPtr(buffer));
        auto pcBuffer = BNode<int, MaxCount, Size>::makeConstFromPtr(Leaf::createBufferPtr(buffer));

        if (touchData) {
            for (int i = 0; i < MaxCount; i++) {
                aNode.addNode(pcBuffer,2,Size * 2 / MaxCount);
            }
        }

        std::vector<ANodePtr> bSrc;
        for (size_t i = 0; i < sizeCap; i++) {
            bSrc.push_back(ANode::createNodePtr(aNode));
        }
        std::vector<ANodePtr> anodes;


        anodes.reserve(sizeCap);
        size_t initialCount = allocator.allocatedCount();
        //
        for (auto _ : state) {
            if (anodes.size() >= sizeCap) {
                state.PauseTiming();
                /*std::cout << "Count before clear " << allocator.allocatedCount() << std::endl;
                std::cout << "Leaf Count before clear " << buffAllocator.allocatedCount() << std::endl;
                std::cout << "Leaf Object Count before clear " << bufferObjectAllocator.allocatedCount() << std::endl;*/
                bSrc.swap(anodes);
                anodes.clear();
                /* std::cout << "Count after clear " << allocator.allocatedCount() << std::endl;
                 std::cout << "Leaf count after clear " << buffAllocator.allocatedCount() << std::endl;
                 std::cout << "Leaf Object Count after clear " << bufferObjectAllocator.allocatedCount() << std::endl;*/
                state.ResumeTiming();
            }
            //size_t currentCount = allocator.allocatedCount();
            //std::cout << "Current count " << currentCount << std::endl;
            auto nodePtr = ANode::createNodePtr(std::move(*bSrc[anodes.size()]));
            anodes.emplace_back(std::move(nodePtr));
        }
        benchmark::DoNotOptimize(anodes);
        anodes.clear();
        // std::cout << "FinalCount " << allocator.allocatedCount() << std::endl;
    }
    allocator.reset();
    buffAllocator.reset();
    bufferObjectAllocator.reset();
    sharedPtrAllocator.reset();

}

#define APPLY_SIZE_TO_BM(BM, COUNT) \
BENCHMARK_TEMPLATE(BM,COUNT,16)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,COUNT,256)->Range(0,1);\
BENCHMARK_TEMPLATE(BM,COUNT,4096)->Range(0,1)
//BENCHMARK_TEMPLATE(BM,COUNT,64)->Range(0,1);\
//BENCHMARK_TEMPLATE(BM,COUNT,1024)->Range(0,1);\


#define APPLY_SIZE_AND_COUNT_TO_BM(BM) \
APPLY_SIZE_TO_BM(BM,4);\
APPLY_SIZE_TO_BM(BM,8);\
APPLY_SIZE_TO_BM(BM,16);\
APPLY_SIZE_TO_BM(BM,32);\
APPLY_SIZE_TO_BM(BM,64)

APPLY_SIZE_AND_COUNT_TO_BM(BM_ANode_Create);
APPLY_SIZE_AND_COUNT_TO_BM(BM_ANode_Move);


//TODO - move this to a utility header
//TODO - create function addAnnotations(annotationsPerNode, annotationSizeForHeight()) - the annotation should just replace a given size with a new tree where values are original + 1Bil.
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



template<bool isTargetBuffer,bool isAnnotationBuffer,class T, size_t MAX_CHILDREN_COUNT, size_t BufferSize>
void annotateNode(typename BNode<T, MAX_CHILDREN_COUNT, BufferSize>::VarType &target,int annotationsPerNode, int annotationSize,int annotationHeight) {
    using ANodeT = ANode<T, MAX_CHILDREN_COUNT, BufferSize>;
    using BNodeT = BNode<T, MAX_CHILDREN_COUNT, BufferSize>;
    using BufferT = Leaf<T, BufferSize>;
    using VarType = typename BNode<T, MAX_CHILDREN_COUNT, BufferSize>::VarType;
    using TargetType = std::conditional_t<isTargetBuffer,std::shared_ptr<const BufferT>,std::shared_ptr<const BNodeT>>;
    auto& targetPtr = std::get<TargetType>(target);
    auto result = ANodeT::createNodePtr(ANodeT(targetPtr));
    auto annotationTree = [&]() -> std::conditional_t<isAnnotationBuffer,BufferT,BNodeT>{
       if constexpr (isAnnotationBuffer) {
           auto r = BufferT::createBuffer(nullptr);
           std::array<T,BufferSize> mockData;
           r.add(mockData.data(),BufferSize);
           return r;
       } else {
           return buildTree<T,MAX_CHILDREN_COUNT,BufferSize>(annotationHeight,MAX_CHILDREN_COUNT,BufferSize);
       };
    }();
    std::vector<int> transfer(annotationSize);
    size_t offset = 0;
    for (int step = 0;step < annotationsPerNode;step++) {
        auto leftOverSize = (targetPtr->size() - annotationsPerNode*annotationSize)*(step+1)/(annotationsPerNode + 1) - (targetPtr->size() - annotationsPerNode*annotationSize)*step/(annotationsPerNode + 1);
        result->addNode(targetPtr,offset,leftOverSize);
        size_t nextOffset = offset+leftOverSize;
        auto localAnnotation = annotationTree;
        auto resultSize = targetPtr->fillBuffer(transfer.data(),nextOffset,annotationSize);
        assert(resultSize == annotationSize);
        for (auto &item : transfer) {
            item += 2000000000;
        }
        size_t offsetOnAnnotationNode = (localAnnotation.size() - annotationSize) / 2;
        localAnnotation.setValues(transfer.data(), offsetOnAnnotationNode, annotationSize);
        localAnnotation.makeConst();
        result->addNode(std::make_shared<const decltype(annotationTree)>(std::move(localAnnotation)),offsetOnAnnotationNode,annotationSize);
        offset = nextOffset + annotationSize;
    }
    result->addNode(targetPtr,offset);
    target = std::shared_ptr<const ANodeT>(std::move(result));
}

template<class T, size_t MAX_CHILDREN_COUNT, size_t BufferSize>
void addAnnotations(BNode<T, MAX_CHILDREN_COUNT, BufferSize> &root,int heightTarget, int annotationsPerNode, int annotationSize,int annotationHeight) {
    assert(heightTarget < root.height());
    std::array<std::shared_ptr<const ANode<T,MAX_CHILDREN_COUNT,BufferSize>>,MAX_CHILDREN_COUNT> newNodes;
    if (root.height() == heightTarget + 1) {
        root.makeConst(true);
        if (annotationHeight) {
            for (int i = 0; i < root.childrenCount(); i++) {
                annotateNode<false, false, T,MAX_CHILDREN_COUNT,BufferSize>(root.childAt(i), annotationsPerNode, annotationSize, annotationHeight);
            }
        } else {
            for (int i = 0; i < root.childrenCount(); i++) {
                if (heightTarget) {
                    annotateNode<false,true,T,MAX_CHILDREN_COUNT,BufferSize>(root.childAt(i), annotationsPerNode, annotationSize, annotationHeight);
                } else {
                    annotateNode<true,true,T,MAX_CHILDREN_COUNT,BufferSize>(root.childAt(i), annotationsPerNode, annotationSize, annotationHeight);
                }
            }
        }
    }
}

template<class Node, class T>
void setValuesANode(Node &dest, T *helperBuffer, size_t offset, size_t length, T initialValue, T step, int bufferSize) {
    T rollingValue = initialValue;
    for (; offset < length; offset += bufferSize) {
        for (size_t valuePos = 0; valuePos < bufferSize; valuePos++, rollingValue += step) {
            helperBuffer[valuePos % bufferSize] = rollingValue;
        }
        dest.setValues(helperBuffer, offset, bufferSize);
    }
}


template<size_t MaxCount, size_t Size>
static void BM_ANode_FillData(benchmark::State &state) {
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
    int heightDelta = state.range(2);
    int height = heightForSize(totalSize, MaxCount, Size);
    if (heightDelta > height) {
        state.SkipWithError("HeightDelta larger than height");
        return;
    }
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


        height = heightForSize(totalSize, MaxCount, Size) - 1;
        std::vector<BNodePtr> bSrc;
        int *helperBuffer = new int[1000];
        for (size_t i = 0; i < sizeCap; i++) {
            auto bNode = buildTree<int, MaxCount, Size>(height, MaxCount, Size);
            setValuesANode(bNode, helperBuffer, 0, bNode.size(), 11, 22, 1000);
            addAnnotations(bNode,height-heightDelta,MaxCount/2 - 1,Size/MaxCount,std::max(0,height-heightDelta - 1));
            bSrc.push_back(BNode::createNodePtr(bNode));
        }
        delete[] helperBuffer;

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
                    assert(readBuffer[i] % 2000000000 == startValue);
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

#define APPLY_SIZE_TO_BM_VAR_STEP(BM, COUNT) \
BENCHMARK_TEMPLATE(BM,COUNT,16)->Ranges({{16,1024},{1024,1024*1024},{3,5}});/*\
BENCHMARK_TEMPLATE(BM,COUNT,64)->Ranges({{64,4096},{4096,4096*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,256)->Ranges({{256,16396},{16396,16396*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,1024)->Ranges({{1024,1024*64},{1024*64,1024*64*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,4096)->Ranges({{4096,4096*64},{4096*64,4096*64*1024}})*/


#define APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP(BM) \
APPLY_SIZE_TO_BM_VAR_STEP(BM,4);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,8);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,16);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,32);\
APPLY_SIZE_TO_BM_VAR_STEP(BM,64)

APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP(BM_ANode_FillData);


template<size_t MaxCount, size_t Size>
static void BM_ANode_FillData1P(benchmark::State &state) {
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
    int height = heightForSize(totalSize, MaxCount, Size);
    if (height <= 1) {
        state.SkipWithError("Tree too low");
        return;
    }
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


        height = heightForSize(totalSize, MaxCount, Size) - 1;
        std::vector<BNodePtr> bSrc;
        int *helperBuffer = new int[1000];
        for (size_t i = 0; i < sizeCap; i++) {
            auto bNode = buildTree<int, MaxCount, Size>(height, MaxCount, Size);
            setValuesANode(bNode, helperBuffer, 0, bNode.size(), 11, 22, 1000);
            addAnnotations(bNode,1,MaxCount/2 - 1,1,0);
            bSrc.push_back(BNode::createNodePtr(bNode));
        }
        delete[] helperBuffer;

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
                    assert(readBuffer[i] % 2000000000 == startValue);
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

#define APPLY_SIZE_TO_BM_VAR_STEP_1(BM, COUNT) \
BENCHMARK_TEMPLATE(BM,COUNT,16)->Ranges({{16,1024},{1024,1024*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,64)->Ranges({{64,4096},{4096,4096*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,256)->Ranges({{256,16396},{16396,16396*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,1024)->Ranges({{1024,1024*64},{1024*64,1024*64*1024}});\
BENCHMARK_TEMPLATE(BM,COUNT,4096)->Ranges({{4096,4096*64},{4096*64,4096*64*1024}})


#define APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP_1(BM) \
APPLY_SIZE_TO_BM_VAR_STEP_1(BM,4);\
APPLY_SIZE_TO_BM_VAR_STEP_1(BM,8);\
APPLY_SIZE_TO_BM_VAR_STEP_1(BM,16);\
APPLY_SIZE_TO_BM_VAR_STEP_1(BM,32);\
APPLY_SIZE_TO_BM_VAR_STEP_1(BM,64)

APPLY_SIZE_AND_COUNT_TO_BM_VAR_STEP_1(BM_ANode_FillData1P);
