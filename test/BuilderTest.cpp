#include "gtest/gtest.h"
#include "../Builder.h"
#include "utilities.h"
#include "TestDataTool.h"

using BuilderT = Builder<int, 16, 16,ArrayAdapter>;
using LeafT = BuilderT::LeafT;
using BNodeT = BuilderT::BNodeT;

//TODO - generate trees of all the combinations and validate balance method 1st -
//TODO - add test cases that hits each branch
//TODO - add test case on cartesian product: (front,back)x(nullBuild,tallerRoot,shorterRoot,equalRoot)x
// (Leaf(balanced,unbalanced),BNode (unBalanced, Balanced, Full), Anode(balancedXunbalanced)X(withSlots,compactable, totallyFull)) X
// (const shared, movable shared, const unique, move unique)

#define SPEED_UP 10

TEST(BuilderTest, creation) {
    BuilderT builder0;
    ASSERT_EQ(builder0.size(), 0);
    auto result0 = builder0.close();
    ASSERT_EQ(BNodeT::sizeOf(result0), 0);

    BuilderT builder;
    builder.addNode(LeafT::createLeafPtr(LeafT::createLeaf(nullptr)));
    ASSERT_EQ(builder.size(), 0);
    auto result = builder.close();
    ASSERT_EQ(BNodeT::sizeOf(result), 0);

    BuilderT builder1;
    LeafT buffer = LeafT::createLeaf(nullptr);
    int data[] = {1, 2, 3};
    buffer.add(data, 1);
    builder1.addNode(LeafT::createLeafPtr(buffer));
    ASSERT_EQ(builder1.size(), 1);
    auto result1 = builder1.close();
    ASSERT_EQ(BNodeT::sizeOf(result1), 1);

    BuilderT builder2;
    BNodeT bNodeT(1);
    bNodeT.addNode(LeafT::createLeafPtr(buildLeaf<int, 16>(10)));
    bNodeT.addNode(LeafT::createLeafPtr(buildLeaf<int, 16>(10)));
    builder2.addNode(BNodeT::createNodePtr(bNodeT));
    ASSERT_EQ(builder2.size(), 20);
    auto result2 = builder2.close();
    ASSERT_EQ(BNodeT::sizeOf(result2), 20);

    //TODO create a simple ANode and add it
    //TODO - create getBNode[C]Ptr(Children...) getLeaf[C]Ptr(int{}), nodeAnnotation(NodePtr, offset,len) -> Annotation,
    // getANode[C]Ptr(OriginPtr,Annotation...)

}

template<class T, size_t MAX_COUNT, size_t SIZE>
struct TArgs {
    using type = T;
    static constexpr size_t MaxCount = MAX_COUNT;
    static constexpr size_t Size = SIZE;
};

template<class TARGS>
class BuilderTTest : public testing::Test, public TDT<typename TARGS::type, TARGS::MaxCount, TARGS::Size> {
public:

    void testCurrentState(auto &&generator, int step = 0) {
        using Type = typename TARGS::type;
        auto b = this->builder();
        Type value = 0;

        auto testSet = generator.getState(value);
        auto expectedData = testSet.first;
        size_t pos = 0;
        for (Type i = 0; i < expectedData.size(); i++) {
            ASSERT_EQ(expectedData[i], valueAt(pos++, testSet.second));
        }
        b.addNode(std::move(testSet.second));
        auto stateId = generator.getStateId();
        const std::string stateIdAsString = ::testing::PrintToString(stateId);
        SCOPED_TRACE("Size = " + std::to_string(expectedData.size()) +
                     " step = " + std::to_string(step) +
                     " state = " + stateIdAsString);
        if (step % 1000 == 0) {
            std::cout << "State: " << stateIdAsString << std::endl;
            std::cout << "Size:" << expectedData.size() << std::endl;
            std::cout << "Step = " << step << std::endl;
        }
        ASSERT_EQ(b.size(), expectedData.size());
        for (Type i = 0; i < expectedData.size(); i++) {
            SCOPED_TRACE("i = " + std::to_string(i));
            ASSERT_EQ(expectedData[i], b[i]);
        }
        try {
            auto res = b.close();
            if (sizeOf(res) != expectedData.size()) {
                ASSERT_EQ(sizeOf(res), expectedData.size());
            }
            pos = 0;
            for (Type i = 0; i < expectedData.size(); i++) {
                SCOPED_TRACE("i = " + std::to_string(i));
                ASSERT_EQ(expectedData[i], valueAt(pos++, res));
            }
            auto isBalanced = [&](auto &&stateId) {
                return isDeepBalanced(res, true);
            };
            EXPECT_PRED1(isBalanced, generator.getStateId());
        } catch (...) {
            EXPECT_FALSE(true);
        }
    }

    void addRatio(auto &&b, auto &&testSet, size_t offset, size_t len, bool asPrefix = false) {
        b.addNode(std::move(testSet.second), offset, len, asPrefix);
        testSet.first = decltype(testSet.first)(testSet.first.begin() + offset, testSet.first.begin() + offset + len);
    }

    void addForStep(auto &&b, auto &&testSet, size_t addMode, bool asPrefix = false) {
        switch (addMode) {
            case 0:
                b.addNode(std::move(testSet.second), asPrefix);
                break;
            case 1:
                addRatio(b, testSet, 1, testSet.first.size() - 1, asPrefix);
                break;
            case 2:
                addRatio(b, testSet, 0, testSet.first.size() - 1, asPrefix);
                break;
            case 3:
                addRatio(b, testSet, 0, testSet.first.size() / 2, asPrefix);
                break;
            case 4:
                addRatio(b, testSet, testSet.first.size() / 4, testSet.first.size() / 2, asPrefix);
                break;
            case 5:
                addRatio(b, testSet, testSet.first.size() / 2, testSet.first.size() / 2, asPrefix);
                break;
            case 6:
                addRatio(b, testSet, 1, testSet.first.size() / 2, asPrefix);
                break;
            case 7:
                if (testSet.first.size() < 4) {
                    b.addNode(std::move(testSet.second), asPrefix);
                } else {
                    addRatio(b, testSet, testSet.first.size() / 4 - 1, testSet.first.size() / 2, asPrefix);
                }
                break;
            case 8:
                if (testSet.first.size() < 2) {
                    b.addNode(std::move(testSet.second), asPrefix);
                } else {
                    addRatio(b, testSet, testSet.first.size() / 2 + 1, testSet.first.size() / 2 - 1, asPrefix);
                }
                break;
        }
    }

    void testAddAndClose(auto &&generator1, auto &&generator2, bool asPrefix, size_t addMode1, size_t addMode2,
                         int step = 0) {
        using Type = typename TARGS::type;
        auto b = this->builder();
        Type value = 0;
        auto stateId1 = generator1.getStateId();
        const std::string stateIdAsString1 = ::testing::PrintToString(stateId1);
        auto testSet1 = generator1.getState(value);
        size_t pos = 0;

        addForStep(b, testSet1, addMode1);
        auto expectedData1 = testSet1.first;

        //0: write full, 1: 1,Size-1, 2: 0, Size-1
        // 3: 0,Size/2, 4: Size/4, Size/2 5: Size/2, Size/2
        // 6: 1,Size/2, 7: Size/4 - 1, Size/2 - 1 8: Size/2+1, Size/2-1
        auto stateId2 = generator2.getStateId();
        const std::string stateIdAsString2 = ::testing::PrintToString(stateId2);

        auto testSet2 = generator2.getState(value);
        pos = 0;

        addForStep(b, testSet2, addMode2, asPrefix);
        auto expectedData2 = testSet2.first;
        SCOPED_TRACE("Size1 = " + std::to_string(expectedData1.size()) +
                     " step = " + std::to_string(step) +
                     " state1 = " + stateIdAsString1 +
                     " Size2 = " + std::to_string(expectedData2.size()) +
                     " state2 = " + stateIdAsString1);

        if (step % 100 == 0) {
            std::cout << "Size1:" << expectedData1.size() << std::endl;
            std::cout << "Size2:" << expectedData2.size() << std::endl;
            std::cout << "Step = " << step << std::endl;
        }
        ASSERT_EQ(b.size(), expectedData1.size() + expectedData2.size());
        for (Type i = 0; i < expectedData1.size(); i++) {
            SCOPED_TRACE("i = " + std::to_string(i));
            ASSERT_EQ(expectedData1[i], b[i + (asPrefix ? expectedData2.size() : 0)]);
        }
        for (Type i = 0; i < expectedData2.size(); i++) {
            SCOPED_TRACE("i = " + std::to_string(i));
            ASSERT_EQ(expectedData2[i], b[i + (asPrefix ? 0 : expectedData1.size())]);
        }
        try {
            auto res = b.close();
            if (sizeOf(res) != expectedData1.size() + expectedData2.size()) {
                ASSERT_EQ(sizeOf(res), expectedData1.size() + expectedData2.size());
            }
            for (Type i = 0; i < expectedData1.size(); i++) {
                SCOPED_TRACE("i = " + std::to_string(i));
                ASSERT_EQ(expectedData1[i], valueAt(i + (asPrefix ? expectedData2.size() : 0), res));
            }
            for (Type i = 0; i < expectedData2.size(); i++) {
                SCOPED_TRACE("i = " + std::to_string(i));
                ASSERT_EQ(expectedData2[i], valueAt(i + (asPrefix ? 0 : expectedData1.size()), res));
            }

            auto isBalanced = [&](auto &&stateId) {
                return isDeepBalanced(res, true);
            };
            EXPECT_PRED1(isBalanced, generator1.getStateId());
        } catch (...) {
            EXPECT_FALSE(true);
        }
    }

    void validateBalancingForState(auto &&generator, StateIdType state) {
        generator.setStateId(state);
        testCurrentState(generator, 0);
    }

    void validateBalancing(auto &&generator, int minSeqSteps, int maxActualSteps) {

        int step = 0;
        do {
            try {
                testCurrentState(generator, step);
                step++;
            } catch (SkipStepException &e) {
                // std::cout << "Skipping step " << step << std::endl;
            }
        } while (step < minSeqSteps ? generator.next() : (generator.randomNext(), step < maxActualSteps));
        std::cout << "Done in at step = " << step << std::endl;
    }

    void
    validateAdditionAndBalancingState(auto &&generator1, auto &&generator2, StateIdType &&state1, StateIdType &&state2,
                                      size_t addMode1, size_t addMode2, bool asPrefix = false) {
        generator1.setStateId(state1);
        generator2.setStateId(state2);
        int step = 0;
        testAddAndClose(generator1, generator2, asPrefix, addMode1, addMode2);
    }

    void validateAdditionAndBalancing(auto &&generator1, auto &&generator2, int minSeqSteps, int maxActualSteps) {

        int step = 0;
        do {
            try {
                auto addMode1 = std::rand() % 9;
                auto addMode2 = std::rand() % 9;
                {
                    SCOPED_TRACE("AsSuffix");
                    testAddAndClose(generator1, generator2, false, addMode1, addMode2, step);
                }
                {
                    SCOPED_TRACE("AsPrefix");
                    testAddAndClose(generator1, generator2, true, addMode1, addMode2, step);
                }
                step++;
            } catch (SkipStepException &e) {
                // std::cout << "Skipping step " << step << std::endl;
            }
        } while (step < minSeqSteps ? (generator1.next(), generator2.next(), step < maxActualSteps) :
                 (generator1.randomNext(), generator2.randomNext(), step < maxActualSteps));
        std::cout << "Done in at step = " << step << std::endl;
    }

};

using testing::Types;

// Google Test offers two ways for reusing tests for different types.
// The first is called "typed tests".  You should use it if you
// already know *all* the types you are gonna exercise when you write
// the tests.

// To write a typed test case, first use
//
//   TYPED_TEST_SUITE(TestCaseName, TypeList);
//
// to declare it and specify the type parameters.  As with TEST_F,
// TestCaseName must match the test fixture name.

// The list of types we want to test.
typedef Types<TArgs<int, 4, 16>/*, TArgs<long, 16, 64>*/> Implementations;

TYPED_TEST_SUITE(BuilderTTest, Implementations);

#define EXPOSE_METHOD(methodName)  auto methodName = [&](auto &&...v) {return this->methodName(std::forward<decltype(v)>(v)...);}


#define THIS_SHORTCUTS     auto p = [&](auto &&v) { return this->p(std::move(v)); };\
    auto cp = [&](auto &&v) { return this->cp(std::move(v)); };\
    auto getLeaf = [&](size_t size, auto offset, auto step) {\
        return this->getLeaf(size, offset, step);\
    };\
    auto getLeafV = [&](std::initializer_list<decltype(this->typeTest())> values) {\
        return this->getLeafV(values);\
    };\
    auto getBNode = [&](auto &&...nodes) {\
        return this->getBNode(nodes ...);\
    };\
    auto getANode = [&](auto &&...nodes) {\
        return this->getANode(nodes ...);\
    };\
    EXPOSE_METHOD(getSlicedOrNotLeafGenerator);\
    auto r = [&](size_t count, const auto &node) { return this->r(count, node); };\
    auto getLeafGenerator = [&](auto &&...v) {return this->getLeafGenerator(std::forward<decltype(v)>(v)...);};\
    auto Size = this->Size;\
    auto MaxCount = this->MaxCount; \
    using BNode = decltype(getBNode());\
    using Leaf = decltype(getLeafV({}));\
    using ANode = decltype(getANode());\
    using T = decltype(this->tValue());\
    auto getPointerGenerator = [&](auto&& generator,bool isRoot = false) {\
        return this->getPointerGenerator(std::forward<decltype(generator)>(generator),isRoot);\
    }


TYPED_TEST(BuilderTTest, testBasicConstructionTools) {
    THIS_SHORTCUTS;
    auto pv = p(getLeaf(Size - 1, 10, 3));
    ASSERT_EQ(pv->size(), Size - 1);
    auto pn = cp(getBNode(
            p(getLeaf(Size - 1, 10, 3)),
            r(MaxCount - 3, p(getLeaf(Size - 1, 100, 30))),
            p(getLeaf(Size - 1, 10000, 10))));
    ASSERT_EQ(pn->size(), (MaxCount - 1) * (Size - 1));

    int pos = 0;
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ((*pn)[pos++], 10 + i * 3);
    }
    for (int j = 0; j < MaxCount - 3; j++) {
        for (int i = 0; i < Size - 1; i++) {
            ASSERT_EQ((*pn)[pos++], 100 + i * 30);
        }
    }
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ((*pn)[pos++], 10000 + i * 10);
    }
}

auto validateSubSizeResults(auto &b, size_t Size) {
    [&]() {
        ASSERT_EQ(b.size(), 2);
        ASSERT_EQ(b.height(), 0);
    }();
    auto res1 = b.close();
    [&]() {
        ASSERT_EQ(sizeOf(res1), 2);
        ASSERT_EQ(heightOf(res1), 0);
        ASSERT_EQ(res1.index(), 3);
    }();
    return res1;
}

void validateSubSizeResultsFinal(auto &payload, size_t Size) {
    ASSERT_EQ(payload[0], 10);
    ASSERT_EQ(payload[1], 20);
    ASSERT_EQ(payload.isBalanced(), Size <= 2 * 2);
    ASSERT_EQ(payload.isConst(), true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSize) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeafV({10}));
    auto pb2 = p(getLeafV({20}));
    auto b = this->builder();
    b.addNode(std::move(pb1));
    b.addNode(std::move(pb2));
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial1) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 1);
    b.addNode(std::move(pb2), 0, 1);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial2) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 1);
    b.addNode(std::move(pb2), 0, 1);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial3) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 1);
    b.addNode(std::move(pb2), 0, 1);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial4) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 1);
    b.addNode(std::move(pb2), 0, 1);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial5) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 0, 1, true);
    b.addNode(std::move(pb1), 1, 1, true);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial6) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 0, 1, true);
    b.addNode(std::move(pb1), 1, 1, true);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial7) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 0, 1, true);
    b.addNode(std::move(pb1), 1, 1, true);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizePartial8) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size - 2, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 0, 1, true);
    b.addNode(std::move(pb1), 1, 1, true);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeBack) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeafV({10}));
    auto pb2 = p(getLeafV({20}));
    auto b = this->builder();
    b.addNode(std::move(pb2), true);
    b.addNode(std::move(pb1), true);
    auto res = validateSubSizeResults(b, Size);
    validateSubSizeResultsFinal(getConst<Leaf>(res), Size);
}

auto validateSubSizeResultsSmallBig(auto &b, size_t Size, int builderHeight, int resultIndex, int resultHeight) {
    [&]() {
        ASSERT_EQ(b.size(), Size);
        ASSERT_EQ(b.height(), builderHeight);
    }();
    auto res1 = b.close();
    [&]() {
        ASSERT_EQ(sizeOf(res1), Size);
        ASSERT_EQ(heightOf(res1), resultHeight);
        ASSERT_EQ(res1.index(), resultIndex);
    }();
    return res1;
}

void validateSubSizeResultsSmallBigFinal(auto &payload, size_t Size, bool isBalanced) {
    ASSERT_EQ(payload[0], 10);
    ASSERT_EQ(payload[1], 13);
    for (int i = 0; i < Size - 2; i++) {
        ASSERT_EQ(payload[i + 2], i * 7 + 27);
    }
    ASSERT_EQ(payload.isBalanced(), isBalanced);
    ASSERT_EQ(payload.isConst(), true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial1) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 2);
    b.addNode(std::move(pb2), 1, Size - 2);
    auto res = validateSubSizeResultsSmallBig(b, Size, 0, 3, 0);
    validateSubSizeResultsSmallBigFinal(getConst<Leaf>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial2) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 2);
    b.addNode(std::move(pb2), 1, Size - 2);
    auto res = validateSubSizeResultsSmallBig(b, Size, 1, 4, 0);
    validateSubSizeResultsSmallBigFinal(getConst<ANode>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial3) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 2);
    b.addNode(std::move(pb2), 1, Size - 2);
    auto res = validateSubSizeResultsSmallBig(b, Size, 0, 3, 0);
    validateSubSizeResultsSmallBigFinal(getConst<Leaf>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial4) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb1), 1, 2);
    b.addNode(std::move(pb2), 1, Size - 2);
    auto res = validateSubSizeResultsSmallBig(b, Size, 1, 3, 0);
    validateSubSizeResultsSmallBigFinal(getConst<Leaf>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial5) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 1, Size - 2, true);
    b.addNode(std::move(pb1), 1, 2, true);
    auto res = validateSubSizeResultsSmallBig(b, Size, 0, 3, 0);
    validateSubSizeResultsSmallBigFinal(getConst<Leaf>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial6) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 1, Size - 2, true);
    b.addNode(std::move(pb1), 1, 2, true);
    auto res = validateSubSizeResultsSmallBig(b, Size, 0, 4, 0);
    validateSubSizeResultsSmallBigFinal(getConst<ANode>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial7) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 2, 7, 3));
    auto pb2 = cp(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 1, Size - 2, true);
    b.addNode(std::move(pb1), 1, 2, true);
    auto res = validateSubSizeResultsSmallBig(b, Size, 0, 4, 0);
    validateSubSizeResultsSmallBigFinal(getConst<ANode>(res), Size, true);
}

TYPED_TEST(BuilderTTest, testLeafToLeafSubSizeSmallBigPartial8) {
    THIS_SHORTCUTS;
    auto pb1 = cp(getLeaf(Size - 2, 7, 3));
    auto pb2 = p(getLeaf(Size, 20, 7));
    auto b = this->builder();
    b.addNode(std::move(pb2), 1, Size - 2, true);
    b.addNode(std::move(pb1), 1, 2, true);
    auto res = validateSubSizeResultsSmallBig(b, Size, 0, 3, 0);
    validateSubSizeResultsSmallBigFinal(getConst<Leaf>(res), Size, true);
}


TYPED_TEST(BuilderTTest, testLeafToLeafSmallBig) {
    THIS_SHORTCUTS;
    auto pb1 = p(getLeaf(Size - 1, 2, 3));
    auto pb2 = p(getLeafV({100}));
    auto b = this->builder();
    b.addNode(std::move(pb1));
    b.addNode(std::move(pb2));
    ASSERT_EQ(b.size(), Size);
    ASSERT_EQ(b.height(), 0);
    auto res1 = b.close();
    ASSERT_EQ(sizeOf(res1), Size);
    ASSERT_EQ(heightOf(res1), 0);
    ASSERT_EQ(res1.index(), 3);
    auto &payload = getConst<Leaf>(res1);
    auto pos = 0;
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ(payload[pos++], 2 + i * 3);
    }
    ASSERT_EQ(payload[pos], 100);
    ASSERT_TRUE(payload.isBalanced());
    ASSERT_TRUE(payload.isConst());
}

TYPED_TEST(BuilderTTest, testBNodeToLeaf0) {
    THIS_SHORTCUTS;
    auto pbn = p(getBNode(
            p(getLeaf(Size - 1, 1, 3)),
            r(MaxCount - 3, cp(getLeaf(Size - 1, 1, 10))),
            p(getLeaf(Size - 1, 10001, 3))));

    auto pb = p(getLeafV({10}));
    auto b = this->builder();
    b.addNode(std::move(pbn));
    b.addNode(std::move(pb));
    [&]() {
        ASSERT_EQ(b.size(), (MaxCount - 1) * (Size - 1) + 1);
        ASSERT_EQ(b.height(), 1);
    }();
    auto res = b.close();
    [&]() {
        ASSERT_EQ(sizeOf(res), (MaxCount - 1) * (Size - 1) + 1);
        ASSERT_EQ(heightOf(res), 1);
        ASSERT_EQ(res.index(), 5);
    }();
    auto payload = getConst<BNode>(res);
    size_t pos = 0;
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ(payload[pos++], 1 + i * 3);
    }
    for (int j = 0; j < MaxCount - 3; j++) {
        for (int i = 0; i < Size - 1; i++) {
            ASSERT_EQ(payload[pos++], 1 + i * 10);
        }
    }
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ(payload[pos++], 10001 + i * 3);
    }
    ASSERT_EQ(payload[pos], 10);
    ASSERT_EQ(payload.isBalanced(), true);
    ASSERT_EQ(payload.isConst(), true);
}

TYPED_TEST(BuilderTTest, testBNodeToLeafExtra) {
    THIS_SHORTCUTS;
 /*   auto b = this->builder();
    auto smallLeaf = p(getLeaf(Size, 3 * (Size - 1), 3));
    smallLeaf->slice(Size - 3, 3);
    b.addNode(std::move(smallLeaf));
    b.addNode(this->cp(getBNode(
            cp(getBNode(cp(getLeaf(Size, 2, 3)),
                        r(3, cp(getLeaf(Size, 2, 3)))))), true), 1, Size * 4 - 1);

    [&]() {
        ASSERT_EQ(b.size(), Size - 1);
        ASSERT_EQ(b.height(), 1);
    }();
    auto res = b.close();
    [&]() {
        ASSERT_EQ(sizeOf(res), (MaxCount - 1) * (Size - 1) + 1);
        ASSERT_EQ(heightOf(res), 1);
        ASSERT_EQ(res.index(), 5);
    }();
    auto payload = getConst<BNode>(res);
    size_t pos = 0;
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ(payload[pos++], 1 + i * 3);
    }
    for (int j = 0; j < MaxCount - 3; j++) {
        for (int i = 0; i < Size - 1; i++) {
            ASSERT_EQ(payload[pos++], 1 + i * 10);
        }
    }
    for (int i = 0; i < Size - 1; i++) {
        ASSERT_EQ(payload[pos++], 10001 + i * 3);
    }
    ASSERT_EQ(payload[pos], 10);
    ASSERT_EQ(payload.isBalanced(), true);
    ASSERT_EQ(payload.isConst(), true);*/
}


TYPED_TEST(BuilderTTest, testBalancingBuff) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(getSlicedOrNotLeafGenerator(false), true), 20000 / SPEED_UP,
                            100000 / SPEED_UP);
}

TYPED_TEST(BuilderTTest, testBalancingBNode1) {
    THIS_SHORTCUTS;

    this->validateBalancing(getPointerGenerator(this->getBNodeGenerator(1, false, false, false), true),
                            20000 / SPEED_UP, 100000 / SPEED_UP);
}

TYPED_TEST(BuilderTTest, testBalancingBNode2) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getBNodeGenerator(2, false, false, false), true), 10,
                            2000000 / Size / SPEED_UP);
}

TYPED_TEST(BuilderTTest, testBalancingBNode3) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getBNodeGenerator(3, false, false, false), true), 10,
                            500000 / Size / SPEED_UP);
}

TYPED_TEST(BuilderTTest, testBalancingBNode4) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getBNodeGenerator(4, false, false, false), true), 10,
                            80000 / Size / SPEED_UP);
}

TYPED_TEST(BuilderTTest, testBalancingANode0) {
    THIS_SHORTCUTS;
    this->validateBalancing(this->getPointerGenerator(this->getANodeGenerator(0)), 10, 1600000 / Size / SPEED_UP);
}

TYPED_TEST(BuilderTTest, testBalancingANode1) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getANodeGenerator(1)), 10, 8000000 / Size);
}

TYPED_TEST(BuilderTTest, testBalancingANode2) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getANodeGenerator(2)), 10, 4000000 / Size);
}

TYPED_TEST(BuilderTTest, testBalancingANode3) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getANodeGenerator(3)), 10,
                            16000000 / MaxCount / MaxCount / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancingANode4) {
    THIS_SHORTCUTS;
    this->validateBalancing(getPointerGenerator(this->getANodeGenerator(4)), 10,
                            4000000 / MaxCount / MaxCount / MaxCount / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancing0) {
    THIS_SHORTCUTS;
    this->validateBalancing(this->getNodePointer(0), 10, 1600000 / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancing1) {
    THIS_SHORTCUTS;
    this->validateBalancing(this->getNodePointer(1), 10, 3200000 / MaxCount / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancing2) {
    THIS_SHORTCUTS;
    this->validateBalancing(this->getNodePointer(2), 10, 6400000 / MaxCount / MaxCount / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancing3) {
    THIS_SHORTCUTS;
    this->validateBalancing(this->getNodePointer(3), 10, 12800000 / MaxCount / MaxCount / MaxCount / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancing4) {
    THIS_SHORTCUTS;
    this->validateBalancing(this->getNodePointer(4), 10,
                            25600000 / MaxCount / MaxCount / MaxCount / MaxCount / MaxCount);
}

TYPED_TEST(BuilderTTest, testBalancingSpec) {
    THIS_SHORTCUTS;
    // this->validateBalancing(getPointerGenerator(getSlicedOrNotLeafGenerator(false), true));
    /* this->validateBalancing(getPointerGenerator(this->getBNodeGenerator(1, false, false, false), true), 20000,
                             100000);*/
    if (Size == 64 && MaxCount == 16) {
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(7153333823811134756,{(0,{(0,(0,{(1,{(0,(0,{(0,{3}),(0,{3}),(0,{4}),(1,{1}),(1,{4}),(0,{3}),(0,{0}),"
                                                "(1,{2})}))})}))}),(0,{(0,0),(0,0),(0,0),(2,(0,{(0,(0,{(1,{3}),(1,{1}),(0,{4}),(1,{0}),(1,{2}),(0,{4}),(0,{3}),"
                                                "(0,{4})}))})),(0,0),(1,(0,{2})),(0,0),(1,(0,{2})),(0,0),(1,(0,{1})),(1,(0,{3})),(1,(0,{4})),(0,0),"
                                                "(2,(0,{(2,(0,{(0,{1}),(0,{2}),(0,{2}),(1,{4}),(1,{0}),(0,{0}),(1,{1}),(1,{0}),(0,{3}),(0,{3})}))})),"
                                                "(2,(0,{(3,(0,{(0,{3}),(0,{1}),(1,{2}),(1,{1}),(0,{4}),(1,{0}),(1,{0}),(0,{4}),(1,{0}),(0,{0}),(0,{4}),(1,{3}),(1,{2}),(1,{0})}))}))})})})"));
    }
    if (Size == 16) {
        this->validateBalancingForState(getPointerGenerator(this->getBNodeGenerator(1, false, false, false), true),
                                        StateIdType::parse("(0,{(1,(0,{(0,{0}),(0,{2})}))})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(3)),
                                        StateIdType::parse(
                                                "(0,{(5800416732607179240,{(0,{(0,(0,{(0,{(0,(0,{(0,{(2,(0,{(1,{1}),(1,{0}),(0,{1}),(1,{2})}))}),(0,{(3,(0,{(0,{0}),(1,{3})}))})}))})}))}),(0,{(2,(0,{(0,(0,{(0,{1}),(0,{4})}))})),(1,(0,{3})),(2,(0,{(1,(0,{(1,{0}),(1,{2}),(1,{3})}))}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(3)),
                                        StateIdType::parse(
                                                "(0,{(1607779961815375566,{(0,{(0,(0,{(1,{(3,(0,{(1,{(4,(0,{(1,{4}),(1,{4}),(1,{4})}))}),(1,{(3,(0,{(1,{1}),(1,{0})}))})}))})}))}),(0,{(1,(0,{3})),(1,(0,{4})),(2,(0,{(1,(0,{(0,{1}),(0,{2}),(1,{3})}))}))})})})"
                                        ));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(3)),
                                        StateIdType::parse(
                                                "(0,{(9114879905184432542,{(0,{(0,(0,{(0,{(3,(0,{(1,{(0,(0,{(0,{4}),(1,{0})}))}),(1,{(0,(0,{(0,{0}),(1,{0})}))})}))})}))}),(0,{(2,(0,{(1,(0,{(0,{4}),(1,{0}),(1,{2})}))})),(2,(0,{(0,(0,{(0,{0}),(0,{1})}))})),(1,(0,{3}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(3)),
                                        StateIdType::parse(
                                                "(0,{(6010422593824409839,{(0,{(0,(0,{(0,{(0,(0,{(0,{(0,(0,{(0,{0}),(1,{3})}))}),(0,{(3,(0,{(1,{3}),(1,{3})}))})}))})}))}),(0,{(3,(0,{(2,(0,{(0,{(1,(0,{(1,{1}),(0,{0}),(1,{3})}))}),(0,{(4,(0,{(0,{0}),(0,{1}),(0,{2})}))}),(1,{(2,(0,{(0,{0}),(0,{4}),(0,{1}),(0,{0})}))}),(1,{(5,(0,{(1,{0}),(0,{0}),(1,{1}),(1,{0})}))})}))})),(2,(0,{(0,(0,{(1,{4}),(1,{4})}))})),(2,(0,{(0,(0,{(0,{4}),(1,{3})}))}))})})})"
                                        ));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(3)),
                                        StateIdType::parse(
                                                "(0,{(8996978633727424660,{(0,{(0,(0,{(0,{(0,(0,{(1,{(3,(0,{(1,{1}),(1,{3})}))}),"
                                                "(1,{(2,(0,{(0,{2}),(1,{0}),(1,{1}),(1,{1})}))})}))})}))}),(0,{(1,(0,{3})),(1,(0,{1})),(1,(0,{3}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(7026054016476713396,{(0,{(1,(0,{(1,{(0,(0,{(1,{3}),(0,{3})}))}),(1,{(0,(0,{(0,{0}),(1,{4})}))})}))}),(0,{(0,0),(2,(0,{(3,(0,{(1,{3}),(1,{2})}))})),(1,(0,{4}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(6006235873385626924,{(0,{(0,(0,{(1,{(0,(0,{(1,{2}),(0,{3})}))})}))}),(0,{(2,(0,{(1,(0,{(1,{0}),(1,{0}),"
                                                "(1,{4})}))})),(2,(0,{(3,(0,{(1,{1}),(0,{3})}))})),(2,(0,{(5,(0,{(0,{4}),(1,{4}),(0,{0}),(1,{3})}))}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(1733836332190588714,{(0,{(0,(0,{(0,{(3,(0,{(1,{3}),(1,{2})}))})}))}),"
                                                "(0,{(2,(0,{(5,(0,{(1,{2}),(0,{1}),(1,{0}),(0,{2})}))})),(1,(0,{3})),(1,(0,{3}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(3230119118725612844,{(0,{(0,(0,{(0,{(3,(0,{(1,{3}),(1,{4})}))})}))}),"
                                                "(0,{(1,(0,{3})),(1,(0,{3})),(2,(0,{(1,(0,{(1,{3}),(1,{3}),(1,{2})}))}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(511971030062399028,{(0,{(5,(0,{(0,{(0,(0,{(0,{4}),(0,{4})}))}),"
                                                "(0,{(0,(0,{(1,{4}),(1,{3})}))})}))}),(0,{(0,0),(0,0),(0,0)})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(9199014814785160606,{(0,{(0,(0,{(0,{(3,(0,{(0,{4}),(1,{3})}))})}))}),(0,{(0,0),(1,(0,{1})),(2,(0,{(0,(0,{(0,{1}),(1,{2})}))}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(7927338992869605405,{(0,{(0,(0,{(1,{(1,(0,{(1,{4}),(1,{3}),(1,{3})}))})}))}),"
                                                "(0,{(0,0),(2,(0,{(0,(0,{(0,{4}),(0,{3})}))})),(1,(0,{1}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse("(0,{(8015821826547454283,{(0,{(0,(0,{(1,{(0,(0,{(0,{3}),"
                                                           "(0,{3})}))})}))}),(0,{(0,0),(2,(0,{(1,(0,{(1,{1}),(1,{4}),"
                                                           "(1,{4})}))})),(0,0)})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(7732245138216377453,{(0,{(0,(0,{(0,{(1,(0,{(0,{3}),(0,{1}),(1,{2})}))})}))}),"
                                                "(0,{(2,(0,{(0,(0,{(1,{1}),(1,{1})}))})),(1,(0,{4})),(1,(0,{0}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(807022526489511032,{(0,{(0,(0,{(0,{(0,(0,{(1,{1}),(1,{0})}))})}))}),(0,{(2,(0,{(1,(0,{(1,{4}),(0,{3}),"
                                                "(1,{4})}))})),(2,(0,{(1,(0,{(1,{1}),(0,{3}),(1,{3})}))})),(0,0)})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(2)),
                                        StateIdType::parse(
                                                "(0,{(4402059746831447025,{(0,{(0,(0,{(1,{(3,(0,{(1,{4}),"
                                                "(1,{4})}))})}))}),(0,{(2,(0,{(1,(0,{(0,{1}),(1,{4}),"
                                                "(0,{2})}))})),(2,(0,{(0,(0,{(0,{4}),(0,{0})}))})),"
                                                "(1,(0,{1}))})})})"));
        this->validateBalancingForState(getPointerGenerator(this->getANodeGenerator(1)),
                                        StateIdType::parse(
                                                "(0,{(5218760779478110070,{(0,{(1,(0,{(0,{3}),(0,{3})}))}),(0,{(1,(0,{4})),(1,(0,{2})),(1,(0,{3}))})})})"));
    }
/*    this->validateBalancingForState(getPointerGenerator(this->getBNodeGenerator(3, false, false, false), true),
                                    StateIdType::parse(
                                            "(0,{(2,(0,{(0,{(0,(0,{(0,{(0,(0,{(0,{1})}))})}))}),(0,{(1,(0,{(1,{(5,(0,{(1,{3}),(1,{4}),(0,{0}),(1,{2})}))}),(0,{(3,(0,{(0,{3}),(1,{2}),(1,{6})}))})}))})}))})"));
    this->validateBalancingForState(getPointerGenerator(this->getBNodeGenerator(1, false, false, false), true),
                                    StateIdType::parse("(0,{(1,(0,{(0,{2}),(0,{0})}))})"));

    this->validateBalancingForState(getPointerGenerator(this->getBNodeGenerator(2, false, false, false), true),
                                    StateIdType::parse(
                                            "(0,{(5,(0,{(0,{(5,(0,{(0,{0}),(1,{3})}))}),(0,{(7,(0,{(0,{3}),(1,{3}),(1,{2}),(0,{3})}))})}))})"));
*/
}

TYPED_TEST(BuilderTTest, testAdding) {
    THIS_SHORTCUTS;
    size_t SizeAll = 200000;
    size_t maxStepsI = SizeAll;
    for (int i = 0; i < 5; i++) {
        auto gen1 = this->getNodePointer(i);
        SCOPED_TRACE("i  = " + std::to_string(i));
        size_t maxStepsJ = SizeAll;
        for (int j = 0; j < 5; j++) {
            // if (i > 1 && j>1) {
            SCOPED_TRACE("j  = " + std::to_string(j));
            std::cout << "i = " << i << " j = " << j << std::endl;
            this->validateAdditionAndBalancing(gen1, this->getNodePointer(j), (j == 0 ? 100 : 0),
                                               std::min(maxStepsJ, maxStepsI));
            // }
            maxStepsJ /= (MaxCount / 2);
        }
        maxStepsI /= (MaxCount / 2);
    }
}

TYPED_TEST(BuilderTTest, testAddingSelect) {
    THIS_SHORTCUTS;
    if (Size == 16 && MaxCount == 4) {
        this->validateAdditionAndBalancingState(
                this->getNodePointer(0),
                this->getNodePointer(1),
                StateIdType::parse("(1,(0,{(2265146176719647357,{(0,{7}),(0,{(1,(0,{10})),(0,0),(1,(0,{4}))})})}))"),
                StateIdType::parse("(0,(0,{(4,(0,{(1,{18}),(0,{10}),(0,{9}),(0,{6})}))}))"), 0, 8);
        this->validateAdditionAndBalancingState(
                this->getNodePointer(0),
                this->getNodePointer(0),
                StateIdType::parse(
                        "(1,(0,{(304404167910257216,{(0,{11}),(0,{(1,(0,{5})),(1,(0,{1})),(1,(0,{9}))})})}))"),
                StateIdType::parse(
                        "(0,(1,{(7,{5})}))"), 5, 0);
        this->validateAdditionAndBalancingState(
                this->getNodePointer(2),
                this->getNodePointer(2),
                StateIdType::parse(
                        "(0,(1,{(1,(0,{(1,{(7,(0,{(0,{13}),(0,{7}),(1,{6}),(0,{1})}))}),(1,{(0,(0,{(0,{7})}))})}))}))"),
                StateIdType::parse(
                        "(0,(1,{(1,(0,{(1,{(7,(0,{(0,{13}),(0,{7}),(1,{6}),(0,{1})}))}),(1,{(0,(0,{(0,{7})}))})}))}))"),
                0, 0);
        this->validateAdditionAndBalancingState(
                this->getNodePointer(0),
                this->getNodePointer(1),
                StateIdType::parse("(0,(0,{(0,{12})}))"),
                StateIdType::parse(
                        "(1,(1,{(4726252789855065373,{(0,{(1,(0,{(0,{7}),(0,{1})}))}),(0,{(1,(0,{8})),(1,(0,{3})),(1,(0,{3}))})})}))"),
                0, 0);
        this->validateAdditionAndBalancingState(
                this->getNodePointer(0),
                this->getNodePointer(1),
                StateIdType::parse("(1,(1,{(8608675511905589538,{(0,{2}),(0,{(1,(0,{5})),(0,0),(1,(0,{8}))})})}))"),
                StateIdType::parse(
                        "(1,(0,{(8506680207266170137,{(0,{(5,(0,{(0,{3}),(1,{6})}))}),(0,{(0,0),(0,0),(0,0)})})}))"), 0,
                0);

        this->validateAdditionAndBalancingState(
                this->getNodePointer(0),
                this->getNodePointer(0),
                StateIdType::parse("(1,(0,{(3573566659235464358,{(0,{4}),(0,{(0,0),(0,0),(1,(0,{8}))})})}))"),
                StateIdType::parse("(1,(0,{(1615477153150923134,{(0,{4}),(0,{(1,(0,{13})),(0,0),(0,0)})})}))"), 0, 0);
    }
}
/*
12
21

12
23

12
31


*/