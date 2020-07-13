#include "utilities.h"




void validateData(BNode<int, 16, 16> &root, int totalSize) {
    ASSERT_EQ(root.size(), totalSize);
    for (size_t i = 0; i < totalSize; i++) {
        const int &v = root[i];
        ASSERT_EQ(v, 10 + i * 3);
    }
    std::array<int, 11> buffer{};
    for (size_t offset = 0; offset < totalSize;) {
        root.fillLeaf(buffer.data(), offset, std::min(buffer.size(), totalSize - offset));
        for (size_t i = 0; i < buffer.size() && offset < totalSize; i++, offset++) {
            ASSERT_EQ(buffer[i], 10 + offset * 3);
        }
    }
    ASSERT_EQ(root.fillLeaf(buffer.data(),root.size(),1),0);
    ASSERT_EQ(root.fillLeaf(buffer.data(),root.size()*2,1),0);
}

void addFlippedNodes1(BNodeT &destNode, const BNodeT &srcNode) {
    for (size_t i = 0; i < srcNode.childrenCount(); i++) {
        destNode.addNode(srcNode.nodeAt(i), true);
    }
    ASSERT_EQ(srcNode.size(), destNode.size());
}

void addFlippedNodes2(BNodeT &destNode, const BNodeT &srcNode) {
    for (int i = srcNode.childrenCount() - 1; i >=0; i--) {
        destNode.addNode(srcNode.nodeAt(i));
    }
    ASSERT_EQ(srcNode.size(), destNode.size());
}

void testFlipped(const BNodeT &root) {
    if (root.height() > 1) {
        for (size_t i = 0; i < root.childrenCount();i++) {
            auto &variant = root.nodeAt(i);
            switch (variant.index()) {
                case 2:
                    testFlipped(*std::get<BNodeT::BNodePtr>(variant));
                    break;
                case 5:
                    testFlipped(*std::get<BNodeT::BNodeCPtr>(variant));
                    break;
                default:
                    ASSERT_TRUE(false);
            }
        }
    }
    BNodeT root2(root.height());
    BNodeT root3(root.height());
    addFlippedNodes1(root2,root);
    addFlippedNodes2(root3,root);
    for (size_t i = 0; i < root2.size(); i++) {
        ASSERT_EQ(root2[i], root3[i]);
    }
    BNodeT root4(root.height());
    BNodeT root5(root.height());
    addFlippedNodes1(root4,root2);
    addFlippedNodes2(root5,root3);
    for (size_t i = 0; i < root.size(); i++) {
        ASSERT_EQ(root[i], root4[i]);
    }
    for (size_t i = 0; i < root.size(); i++) {
        ASSERT_EQ(root[i], root5[i]);
    }
}

void setAndGetFlippedTest(int level, int childrenCount, int bufferSize, bool addPrefix) {
    auto root = buildTree<int, 16, 16>(level, childrenCount, bufferSize, addPrefix);
    int totalSize = bufferSize;
    for (int i = 0; i < level; i++) {
        totalSize *= childrenCount;
    }
    setValues(root, 0, totalSize, 10, 3, 1000);
    validateData(root, totalSize);
    testFlipped(root);
    auto root2 = root;
    testFlipped(root2);
    auto root2m = std::move(root2);
    testFlipped(root2m);
    root.makeConst(true);
    testFlipped(root);
    auto root3 = root;
    testFlipped(root);
    auto root4 = std::move(root3);
    testFlipped(root4);
}


void setAndGetTest(int level, int childrenCount, int bufferSize, bool addPrefix) {
    auto root = buildTree<int, 16, 16>(level, childrenCount, bufferSize, addPrefix);
    int totalSize = bufferSize;
    for (int i = 0; i < level; i++) {
        totalSize *= childrenCount;
    }

    setValues(root, 0, totalSize, 10, 3, 1000);
    validateData(root, totalSize);
    auto root2 = root;
    for (int i = 0; i < childrenCount; i++) {
        ASSERT_NE(root.nodeAt(i), root2.nodeAt(i));
    }
    validateData(root2, totalSize);
    auto root2m = std::move(root2);
    ASSERT_EQ(root2.size(), 0);
    validateData(root2m, totalSize);
    root.makeConst(true);
    validateData(root, totalSize);
    auto root3 = root;
    validateData(root3, totalSize);
    for (int i = 0; i < childrenCount; i++) {
        ASSERT_EQ(root.nodeAt(i), root3.nodeAt(i));
        ASSERT_NE(root.nodeAt(i), root2m.nodeAt(i));
    }
    auto root4 = std::move(root3);
    validateData(root4, totalSize);
    EXPECT_THROW(root4.setValues(new int[1],0,1),std::logic_error);
}
