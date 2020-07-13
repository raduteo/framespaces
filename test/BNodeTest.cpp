#include "gtest/gtest.h"
#include "utilities.h"
#include "../BNode.h"
#include "../ANode.h"

#include <iostream>
#include <type_traits>



TEST(BNodeTest, constructors) {
    BNodeT bNode(1);
    using Leaf = Leaf<int, 16>;
    auto buffer = Leaf::createLeaf(nullptr);
    int sampleData[] = {1, 2, 3, 4, 1, 2, 3, 4};
    buffer.add(sampleData, 8);
    bNode.addNode(Leaf::createLeafPtr(buffer));
    auto newBufPtr = Leaf::createLeafPtr(buffer);
    newBufPtr->makeConst();
    bNode.addNode(std::shared_ptr<const Leaf>(std::move(newBufPtr)));
    //Node is created mutable
    ASSERT_FALSE(bNode.isConst());
    //Node is not balanced since it has only one child
    ASSERT_FALSE(bNode.isBalanced());
    //Size matches the size of the inserted buffer
    ASSERT_EQ(bNode.size(), 16);
    //Copy constructor
    auto bNode2 = bNode;
    ASSERT_FALSE(bNode.isConst());
    ASSERT_FALSE(bNode2.isConst());

    //copy constructor does a deep copy of all mutated nodes and shallow copy of const nodes
    ASSERT_NE(bNode.nodeAt(0), bNode2.nodeAt(0));
    ASSERT_EQ(bNode.nodeAt(1), bNode2.nodeAt(1));

    bNode.makeConst();
    //Nodes already const are not affected
    ASSERT_EQ(bNode.nodeAt(1), bNode2.nodeAt(1));

    ASSERT_TRUE(bNode.isConst());
    ASSERT_FALSE(bNode2.isConst());
    ASSERT_EQ(bNode2.size(), 16);
    auto bufferPtr = Leaf::createLeafPtr(buffer);
    bNode.addNode(bufferPtr);
    ASSERT_EQ(bNode.size(), 24);
    ASSERT_EQ(bNode2.size(), 16);
    auto bNode3 = std::move(bNode2);
    ASSERT_EQ(bNode2.size(), 0);
    ASSERT_EQ(bNode3.size(), 16);

    ASSERT_FALSE(bNode3.isConst());
    bNode3.makeConst();
    ASSERT_EQ(bNode3.size(), 16);
    ASSERT_TRUE(bNode3.isConst());

    auto bNode4 = bNode3;
    ASSERT_EQ(bNode4.size(), 16);
    ASSERT_TRUE(bNode4.isConst());
}

TEST(BNodeTest, setAndGet) {
    bool addPrefix = false;
    for (size_t i = 0; i < 2; i++) {
        setAndGetTest(1, 10, 14, addPrefix);
        setAndGetTest(2, 10, 14, addPrefix);
        setAndGetTest(3, 10, 14, addPrefix);
        setAndGetTest(1, 8, 14, addPrefix);
        setAndGetTest(2, 8, 14, addPrefix);
        setAndGetTest(3, 8, 14, addPrefix);
        setAndGetTest(1, 8, 8, addPrefix);
        setAndGetTest(2, 8, 8, addPrefix);
        setAndGetTest(3, 8, 8, addPrefix);
        addPrefix = !addPrefix;
    }
}

TEST(BNodeTest, setAndGetFlipped) {
    bool addPrefix = false;
    for (size_t i = 0; i < 2; i++) {
        setAndGetFlippedTest(1, 10, 14, addPrefix);
        setAndGetFlippedTest(2, 10, 14, addPrefix);
        setAndGetFlippedTest(3, 10, 14, addPrefix);
        setAndGetFlippedTest(1, 8, 14, addPrefix);
        setAndGetFlippedTest(2, 8, 14, addPrefix);
        setAndGetFlippedTest(3, 8, 14, addPrefix);
        setAndGetFlippedTest(1, 8, 8, addPrefix);
        setAndGetFlippedTest(2, 8, 8, addPrefix);
        setAndGetFlippedTest(3, 8, 8, addPrefix);
        addPrefix = !addPrefix;
    }
}
