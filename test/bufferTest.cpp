#include "gtest/gtest.h"
#include "../Leaf.h"

struct Foo {
    ~Foo() {
        std::cout<<"Deleting foo"<<std::endl;
    }
};


TEST(LeafTest, creation) {
    auto b1 = Leaf<int,32>::createLeaf(nullptr);
    auto b2 = Leaf<int,32>::createLeaf(nullptr);
    int sampleData[] = {1,2,3,4};
    b2.add(sampleData,4);
    for (int i = 0;i < 4;i++) {
        sampleData[i] *= 10;
    }
    GTEST_ASSERT_EQ((int)b2[0],1);
    GTEST_ASSERT_EQ((int)b2[1],2);
    GTEST_ASSERT_EQ(b2[2],3);
    GTEST_ASSERT_EQ(b2[3],4);
    b1.add(b2,0,10);
    for (int i = 0;i < 4;i++) {
        b2.setAt(i,b2[i] * 10);
    }
    GTEST_ASSERT_EQ(b2[0],10);
    GTEST_ASSERT_EQ(b2[1],20);
    GTEST_ASSERT_EQ(b2[2],30);
    GTEST_ASSERT_EQ(b2[3],40);
    GTEST_ASSERT_EQ(b1[0],1);
    GTEST_ASSERT_EQ(b1[1],2);
    GTEST_ASSERT_EQ(b1[2],3);
    GTEST_ASSERT_EQ(b1[3],4);

    auto b3 = b1;
    GTEST_ASSERT_EQ(b3.at(0),1);
    GTEST_ASSERT_EQ(b3.at(1),2);
    GTEST_ASSERT_EQ(b3.at(2),3);
    GTEST_ASSERT_EQ(b3.at(3),4);
    for (int i = 0;i < 4;i++) {
        b1.setAt(i,b1[i] * 10);
    }
    GTEST_ASSERT_EQ(b3.at(0),1);
    GTEST_ASSERT_EQ(b3.at(1),2);
    GTEST_ASSERT_EQ(b3.at(2),3);
    GTEST_ASSERT_EQ(b3.at(3),4);

    GTEST_ASSERT_EQ(b1.isNull(),false);
    GTEST_ASSERT_EQ(b1.isMutable(),true);
    GTEST_ASSERT_EQ(b3.isMutable(),true);
    b3.makeConst();
    GTEST_ASSERT_EQ(b3.isMutable(),false);
    GTEST_ASSERT_EQ(b3.at(0),1);
    GTEST_ASSERT_EQ(b3.at(1),2);
    GTEST_ASSERT_EQ(b3.at(2),3);
    GTEST_ASSERT_EQ(b3.at(3),4);

    auto b4 = std::move(b1);
    GTEST_ASSERT_EQ(b4.isMutable(),true);
    GTEST_ASSERT_EQ(b1.isNull(),true);
    b3.mutate(nullptr);
    GTEST_ASSERT_EQ(b3.isMutable(),true);
}
/*
 Vanilla BTree Node + Annotated Node
 ChildType - Variant<[const]Annotated/Node/Buf>

 */