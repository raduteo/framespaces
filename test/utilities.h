#ifndef EXPERIMENTS_UTILITIES_H
#define EXPERIMENTS_UTILITIES_H
#include "../BNode.h"
#include "../ANode.h"
#include <iostream>
#include "gtest/gtest.h"

using LeafT = Leaf<int, 16>;
using BNodeT = BNode<int, 16, 16>;
using ANodeT = ANode<int, 16, 16>;

template<class T, size_t LeafSize>
Leaf<T, LeafSize> buildLeaf(int bufferSize) {
    const static std::array<T, LeafSize> mockLeaf{};
    auto buffer = Leaf<T,LeafSize>::createLeaf(nullptr);
    buffer.add(mockLeaf.data(), bufferSize);
    return buffer;
}

template<class T, size_t MAX_CHILDREN_COUNT, size_t LeafSize>
BNode<T, MAX_CHILDREN_COUNT, LeafSize> buildTree(int level, int childrenCount, int bufferSize, bool addPrefix = true) {
    using BNode = BNode<T, MAX_CHILDREN_COUNT, LeafSize>;
    using Leaf = Leaf<T, LeafSize>;
    BNode result(level);
    if (level == 1) {
        auto buffer = buildLeaf<T,LeafSize>(bufferSize);
        for (size_t i = 0; i < childrenCount; i++) {
            result.addNode(Leaf::createLeafPtr(buffer), addPrefix);
        }
    } else {
        for (size_t i = 0; i < childrenCount; i++) {
            result.addNode(BNode::createNodePtr(
                    buildTree<T, MAX_CHILDREN_COUNT, LeafSize>(level - 1, childrenCount, bufferSize, addPrefix)));
        }
    }
    return result;
}


template<class Node, class T>
void setValues(Node &dest, size_t offset, size_t length, T initialValue, T step, int bufferSize) {
    std::vector<T> helperLeaf;
    helperLeaf.resize(bufferSize);
    size_t pos = 0;
    for (T rollingValue = initialValue; pos < length; pos++, rollingValue += step) {
        helperLeaf[pos % helperLeaf.size()] = rollingValue;
        if ((pos + 1) % helperLeaf.size() == 0) {
            dest.setValues(helperLeaf.data(), offset, helperLeaf.size());
            offset += helperLeaf.size();
        }
    }
    if (pos % helperLeaf.size()) {
        dest.setValues(helperLeaf.data(), offset, pos % helperLeaf.size());
    }
    ASSERT_EQ(dest.setValues(helperLeaf.data(),dest.size(),1),0);
    ASSERT_EQ(dest.setValues(helperLeaf.data(),dest.size()*2,1),0);
}

void validateData(BNode<int, 16, 16> &root, int totalSize);
void addFlippedNodes1(BNodeT &destNode, const BNodeT &srcNode);
void addFlippedNodes2(BNodeT &destNode, const BNodeT &srcNode);
void testFlipped(const BNodeT &root);
void setAndGetFlippedTest(int level, int childrenCount, int bufferSize, bool addPrefix) ;
void setAndGetTest(int level, int childrenCount, int bufferSize, bool addPrefix);


#endif //EXPERIMENTS_UTILITIES_H
