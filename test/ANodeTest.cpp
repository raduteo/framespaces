#include "gtest/gtest.h"
#include "../BNode.h"
#include "../ANode.h"
#include "../Builder.h"
#include "utilities.h"

#include <iostream>
#include <type_traits>
#include <experimental/type_traits>


TEST(ANodeTest, wrappingBNode) {
    BNodeT bNode(1);
    using Leaf = Leaf<int, 16>;
    auto buffer = Leaf::createLeaf(nullptr);
    int sampleData[8];
    for (int i = 0; i < 8; i++) {
        sampleData[i] = i + 1;
    }
    buffer.add(sampleData, 8);
    bNode.addNode(Leaf::createLeafPtr(buffer));
    auto newBufPtr = Leaf::createLeafPtr(buffer);
    for (int i = 0; i < 8; i++) {
        sampleData[i] = i + 9;
    }
    newBufPtr->setValues(sampleData, 0, 8);
    newBufPtr->makeConst();
    bNode.addNode(std::shared_ptr<const Leaf>(std::move(newBufPtr)));
    int startValue = 16;
    for (int i = 0; i < 6; i++) {
        bNode.addNode(Leaf::createLeafPtr(buffer));
        for (int j = 0; j < 8; j++) {
            sampleData[j] = ++startValue;
        }
        bNode.setValues(sampleData, (i + 2) * 8, 8);
    }
    bNode.makeConst();
    auto pbNode = BNodeT::createNodePtr(std::move(bNode));
    std::shared_ptr<const BNodeT> bnodePtr = std::shared_ptr<const BNodeT>(std::move(pbNode));
    ANodeT aNode(bnodePtr);
    ASSERT_EQ(aNode.childrenCount(), 0);
    ASSERT_EQ(aNode.size(), 0);
    //ANode is not balanced since it has no children
    ASSERT_FALSE(aNode.isBalanced());
    aNode.addNode(bnodePtr);
    //Doing a plane mock mutation
    ASSERT_TRUE(aNode.isBalanced());
    ASSERT_EQ(aNode.size(), bnodePtr->size());
    ASSERT_EQ(aNode.size(), 64);
    //TODO addNode with offset,len and prefix/suffix

    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(aNode[i], i + 1);
    }
    //Adding node with an offset
    ANodeT aNode1(bnodePtr);
    aNode1.addNode(bnodePtr, 1);
    ASSERT_EQ(aNode1.size(), 63);
    ASSERT_FALSE(aNode1.isBalanced());
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode1[i], i + 2);
    }

    //Adding node with a length trim
    ANodeT aNode2(bnodePtr);
    aNode2.addNode(bnodePtr, 0, 63);
    ASSERT_EQ(aNode2.size(), 63);
    ASSERT_FALSE(aNode2.isBalanced());
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i], i + 1);
    }

    auto node = buildTree<int, 16, 16>(1, 10, 10);

    std::shared_ptr<const BNodeT> nodeP = std::make_shared<const BNodeT>(node);
    ASSERT_FALSE(aNode2.canAcceptNode(nodeP));
    ASSERT_TRUE(aNode2.canAcceptNode(nodeP, false, 1000));
    ASSERT_TRUE(aNode2.canAcceptNode(nodeP, false, 0, 0));
    node.makeConst(false);
    std::shared_ptr<const BNodeT> nodeP1 = std::make_shared<const BNodeT>(node);
    ASSERT_FALSE(aNode2.canAcceptNode(nodeP));

    auto node2 = buildLeaf<int, 16>(10);
    for (int i = 0; i < 10; i++) {
        node2.setAt(i, 10 * (i + 1));
    }
    std::shared_ptr<const LeafT> nodeP2 = std::make_shared<const LeafT>(node2);
    ASSERT_TRUE(aNode2.canAcceptNode(nodeP2));
    ASSERT_THROW(aNode2.addNode(std::make_shared<const LeafT>(node2)), std::logic_error);
    node2.makeConst();
    std::shared_ptr<const LeafT> nodeP3 = std::make_shared<const LeafT>(node2);
    ASSERT_TRUE(aNode2.canAcceptNode(nodeP2));
    aNode2.addNode(nodeP3);
    ASSERT_FALSE(aNode2.isBalanced());
    aNode2.addNode(bnodePtr, 63);
    ASSERT_TRUE(aNode2.isBalanced());
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 62], i * 10);
    }
    ASSERT_EQ(aNode2[73], 64);

    aNode2.addNode(nodeP3, 3, 5, true);
    ASSERT_EQ(aNode2.size(), 79);
    ASSERT_EQ(aNode2.childrenCount(), 4);
    auto destVector = std::vector<int>(79);
    aNode2.fillLeaf(destVector.data(), 0, aNode2.size());
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 40);
        ASSERT_EQ(destVector[i], i * 10 + 40);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 5], i + 1);
        ASSERT_EQ(destVector[i + 5], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 67], i * 10);
        ASSERT_EQ(destVector[i + 67], i * 10);
    }
    ASSERT_EQ(aNode2[78], 64);

    aNode2.addNode(nodeP3, 1, 2, true);
    ASSERT_EQ(aNode2.size(), 81);
    ASSERT_EQ(aNode2.childrenCount(), 4);

    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 20);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 7], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 69], i * 10);
    }
    ASSERT_EQ(aNode2[80], 64);

    aNode2.addNode(nodeP3, 1, 4);
    ASSERT_EQ(aNode2.size(), 85);
    ASSERT_EQ(aNode2.childrenCount(), 5);

    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 20);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 7], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 69], i * 10);
    }
    ASSERT_EQ(aNode2[80], 64);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(aNode2[i + 81], i * 10 + 20);
    }

    aNode2.addNode(nodeP3, 5);
    ASSERT_EQ(aNode2.size(), 90);
    ASSERT_EQ(aNode2.childrenCount(), 5);

    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 20);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 7], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 69], i * 10);
    }
    ASSERT_EQ(aNode2[80], 64);
    for (int i = 0; i < 9; i++) {
        ASSERT_EQ(aNode2[i + 81], i * 10 + 20);
    }

    auto aNode3 = aNode2;
    for (int q = 0; q < 55; q++) {
        aNode2.addNode(nodeP3, (q % 5) * 2, 2);
        ASSERT_EQ(aNode2.size(), 92 + q * 2);
        ASSERT_EQ(aNode2.childrenCount(), 6 + q / 5);

        for (int i = 0; i < 7; i++) {
            ASSERT_EQ(aNode2[i], i * 10 + 20);
        }
        for (int i = 0; i < 63; i++) {
            ASSERT_EQ(aNode2[i + 7], i + 1);
        }
        for (int i = 1; i <= 10; i++) {
            ASSERT_EQ(aNode2[i + 69], i * 10);
        }
        ASSERT_EQ(aNode2[80], 64);
        for (int i = 0; i < 9; i++) {
            ASSERT_EQ(aNode2[i + 81], i * 10 + 20);
        }
        for (int i = 0; i < q * 2; i++) {
            ASSERT_EQ(aNode2[i + 90], (i % 10) * 10 + 10);
        }
    }

    for (int q = 0; q < 55; q++) {
        aNode3.addNode(nodeP3, 8 - (q % 5) * 2, 2, true);
        ASSERT_EQ(aNode3.size(), 92 + q * 2);
        ASSERT_EQ(aNode3.childrenCount(), 6 + q / 5);
        std::vector<int> straightData(aNode3.size());
        std::vector<int> batchedData(aNode3.size());
        int step = 1 + q % 10;
        aNode3.fillLeaf(straightData.data(), 0, straightData.size());
        for (int p = 0; p < ((aNode3.size() / step) + (aNode3.size() % step ? 1 : 0)); p++) {
            aNode3.fillLeaf(batchedData.data() + p * step, p * step, step);
        }
        for (int i = 0; i < (q + 1) * 2; i++) {
            ASSERT_EQ(aNode3[i], ((i + 8 - (q % 5) * 2) % 10) * 10 + 10);
        }

        for (int i = 0; i < 7; i++) {
            ASSERT_EQ(aNode3[i + (q + 1) * 2], i * 10 + 20);
        }
        for (int i = 0; i < 63; i++) {
            ASSERT_EQ(aNode3[i + 7 + (q + 1) * 2], i + 1);
        }
        for (int i = 1; i <= 10; i++) {
            ASSERT_EQ(aNode3[i + 69 + (q + 1) * 2], i * 10);
        }
        ASSERT_EQ(aNode3[80 + (q + 1) * 2], 64);
        for (int i = 0; i < 9; i++) {
            ASSERT_EQ(aNode3[i + 81 + (q + 1) * 2], i * 10 + 20);
        }
        for (int i = 0; i < aNode3.size(); i++) {
            ASSERT_EQ(aNode3[i], straightData[i]);
            ASSERT_EQ(aNode3[i], batchedData[i]);
            ASSERT_EQ(straightData[i], batchedData[i]);
        }
    }

    auto aNode4 = ANodeT(std::move(aNode2));
    ASSERT_EQ(aNode2.childrenCount(), 0);
    ASSERT_EQ(aNode2.size(), 0);
    ASSERT_EQ(aNode3.size(), 200);
    ASSERT_EQ(aNode3.childrenCount(), 16);

}

using Leaf8 = Leaf<int, 128>;

TEST(ANodeTest, wrappingLeaf) {

    using Leaf = Leaf8;
    using ANode8 = ANode<int, 16, 128>;
    auto bufferNode = Leaf::createLeaf(nullptr);
    int sampleData[8];
    for (int i = 0; i < 8; i++) {
        sampleData[i] = i + 1;
    }
    bufferNode.add(sampleData, 8);
    for (int i = 0; i < 8; i++) {
        sampleData[i] = i + 9;
    }
    bufferNode.add(sampleData, 8);
    int startValue = 16;
    for (int i = 0; i < 6; i++) {
        for (int j = 0; j < 8; j++) {
            sampleData[j] = ++startValue;
        }
        bufferNode.add(sampleData, 8);
    }
    bufferNode.makeConst();
    std::shared_ptr<const Leaf8> bnodePtr = std::make_shared<const Leaf8>(std::move(bufferNode));
    ANode8 aNode(bnodePtr);
    ASSERT_EQ(aNode.childrenCount(), 0);
    ASSERT_EQ(aNode.size(), 0);
    //ANode is not balanced since it has no children
    ASSERT_FALSE(aNode.isBalanced());
    aNode.addNode(bnodePtr);
    //Doing a plane mock mutation
    ASSERT_TRUE(aNode.isBalanced());
    ASSERT_EQ(aNode.size(), bnodePtr->size());
    ASSERT_EQ(aNode.size(), 64);
    //TODO addNode with offset,len and prefix/suffix

    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(aNode[i], i + 1);
    }
    //Adding node with an offset
    ANode8 aNode1(bnodePtr);
    aNode1.addNode(bnodePtr, 1);
    ASSERT_EQ(aNode1.size(), 63);
    ASSERT_FALSE(aNode1.isBalanced());
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode1[i], i + 2);
    }

    //Adding node with a length trim
    ANode8 aNode2(bnodePtr);
    aNode2.addNode(bnodePtr, 0, 63);
    ASSERT_EQ(aNode2.size(), 63);
    ASSERT_FALSE(aNode2.isBalanced());
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i], i + 1);
    }

    auto node = buildLeaf<int, 128>(100);

    std::shared_ptr<const Leaf8> nodeP = std::make_shared<const Leaf8>(node);
    ASSERT_FALSE(aNode2.canAcceptNode(nodeP));
    node.makeConst();
    std::shared_ptr<const Leaf8> nodeP1 = std::make_shared<const Leaf8>(node);
    ASSERT_FALSE(aNode2.canAcceptNode(nodeP));

    auto node2 = buildLeaf<int, 128>(10);
    for (int i = 0; i < 10; i++) {
        node2.setAt(i, 10 * (i + 1));
    }
    std::shared_ptr<const Leaf8> nodeP2 = std::make_shared<const Leaf8>(node2);
    ASSERT_TRUE(aNode2.canAcceptNode(nodeP2));
    node2.makeConst();
    std::shared_ptr<const Leaf8> nodeP3 = std::make_shared<const Leaf8>(node2);
    ASSERT_TRUE(aNode2.canAcceptNode(nodeP2));
    aNode2.addNode(nodeP3);
    ASSERT_FALSE(aNode2.isBalanced());
    aNode2.addNode(bnodePtr, 63);
    ASSERT_TRUE(aNode2.isBalanced());
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 62], i * 10);
    }
    ASSERT_EQ(aNode2[73], 64);

    aNode2.addNode(nodeP3, 3, 5, true);
    ASSERT_EQ(aNode2.size(), 79);
    ASSERT_EQ(aNode2.childrenCount(), 4);
    auto destVector = std::vector<int>(79);
    aNode2.fillLeaf(destVector.data(), 0, aNode2.size());
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 40);
        ASSERT_EQ(destVector[i], i * 10 + 40);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 5], i + 1);
        ASSERT_EQ(destVector[i + 5], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 67], i * 10);
        ASSERT_EQ(destVector[i + 67], i * 10);
    }
    ASSERT_EQ(aNode2[78], 64);

    aNode2.addNode(nodeP3, 1, 2, true);
    ASSERT_EQ(aNode2.size(), 81);
    ASSERT_EQ(aNode2.childrenCount(), 4);

    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 20);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 7], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 69], i * 10);
    }
    ASSERT_EQ(aNode2[80], 64);

    aNode2.addNode(nodeP3, 1, 4);
    ASSERT_EQ(aNode2.size(), 85);
    ASSERT_EQ(aNode2.childrenCount(), 5);

    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 20);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 7], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 69], i * 10);
    }
    ASSERT_EQ(aNode2[80], 64);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(aNode2[i + 81], i * 10 + 20);
    }

    aNode2.addNode(nodeP3, 5);
    ASSERT_EQ(aNode2.size(), 90);
    ASSERT_EQ(aNode2.childrenCount(), 5);

    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(aNode2[i], i * 10 + 20);
    }
    for (int i = 0; i < 63; i++) {
        ASSERT_EQ(aNode2[i + 7], i + 1);
    }
    for (int i = 1; i <= 10; i++) {
        ASSERT_EQ(aNode2[i + 69], i * 10);
    }
    ASSERT_EQ(aNode2[80], 64);
    for (int i = 0; i < 9; i++) {
        ASSERT_EQ(aNode2[i + 81], i * 10 + 20);
    }

    auto aNode3 = aNode2;
    for (int q = 0; q < 55; q++) {
        aNode2.addNode(nodeP3, (q % 5) * 2, 2);
        ASSERT_EQ(aNode2.size(), 92 + q * 2);
        ASSERT_EQ(aNode2.childrenCount(), 6 + q / 5);

        for (int i = 0; i < 7; i++) {
            ASSERT_EQ(aNode2[i], i * 10 + 20);
        }
        for (int i = 0; i < 63; i++) {
            ASSERT_EQ(aNode2[i + 7], i + 1);
        }
        for (int i = 1; i <= 10; i++) {
            ASSERT_EQ(aNode2[i + 69], i * 10);
        }
        ASSERT_EQ(aNode2[80], 64);
        for (int i = 0; i < 9; i++) {
            ASSERT_EQ(aNode2[i + 81], i * 10 + 20);
        }
        for (int i = 0; i < q * 2; i++) {
            ASSERT_EQ(aNode2[i + 90], (i % 10) * 10 + 10);
        }
    }

    for (int q = 0; q < 55; q++) {
        aNode3.addNode(nodeP3, 8 - (q % 5) * 2, 2, true);
        ASSERT_EQ(aNode3.size(), 92 + q * 2);
        ASSERT_EQ(aNode3.childrenCount(), 6 + q / 5);
        std::vector<int> straightData(aNode3.size());
        std::vector<int> batchedData(aNode3.size());
        int step = 1 + q % 10;
        aNode3.fillLeaf(straightData.data(), 0, straightData.size());
        for (int p = 0; p < ((aNode3.size() / step) + (aNode3.size() % step ? 1 : 0)); p++) {
            aNode3.fillLeaf(batchedData.data() + p * step, p * step, step);
        }
        for (int i = 0; i < (q + 1) * 2; i++) {
            ASSERT_EQ(aNode3[i], ((i + 8 - (q % 5) * 2) % 10) * 10 + 10);
        }

        for (int i = 0; i < 7; i++) {
            ASSERT_EQ(aNode3[i + (q + 1) * 2], i * 10 + 20);
        }
        for (int i = 0; i < 63; i++) {
            ASSERT_EQ(aNode3[i + 7 + (q + 1) * 2], i + 1);
        }
        for (int i = 1; i <= 10; i++) {
            ASSERT_EQ(aNode3[i + 69 + (q + 1) * 2], i * 10);
        }
        ASSERT_EQ(aNode3[80 + (q + 1) * 2], 64);
        for (int i = 0; i < 9; i++) {
            ASSERT_EQ(aNode3[i + 81 + (q + 1) * 2], i * 10 + 20);
        }
        for (int i = 0; i < aNode3.size(); i++) {
            ASSERT_EQ(aNode3[i], straightData[i]);
            ASSERT_EQ(aNode3[i], batchedData[i]);
            ASSERT_EQ(straightData[i], batchedData[i]);
        }
    }

    auto aNode4 = ANode8(std::move(aNode2));
    ASSERT_EQ(aNode2.childrenCount(), 0);
    ASSERT_EQ(aNode2.size(), 0);
    ASSERT_EQ(aNode3.size(), 200);
    ASSERT_EQ(aNode3.childrenCount(), 16);

}

