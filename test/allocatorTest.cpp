#include "gtest/gtest.h"
#include "../FixedSizeAllocator.h"

TEST(FixedSizeAllocator, allocateAndDeallocateAll) {
    auto &fixedSizeAllocator = FixedSizeAllocator<64>::oneAndOnly();
    std::vector<void *> ptrs;
    for (size_t step = 0; step < 10; step++) {
        for (size_t i = 0; i < 10000; i++) {
            ASSERT_EQ(fixedSizeAllocator.allocatedCount(), i);
            void *x = fixedSizeAllocator.alloc();
            ptrs.push_back(x);
            for (int k = 0; k < 8; k++) {
                ((uint64_t *) x)[k] = step * 1000000 + i * 10 + k;
            }
        }
        ASSERT_EQ(fixedSizeAllocator.allocatedCount(), 10000);
        for (size_t i = 0; i < 10000; i++) {
            for (int k = 0; k < 8; k++) {
                ASSERT_EQ(((uint64_t *) ptrs[i])[k], step * 1000000 + i * 10 + k);
            }
            ASSERT_EQ(fixedSizeAllocator.allocatedCount(), 10000 - i);
            fixedSizeAllocator.free(ptrs[i]);
        }
        ptrs.clear();
        ASSERT_EQ(fixedSizeAllocator.allocatedCount(), 0);
    }
}

struct Buf64 {
    int64_t data[8];
};

TEST(StdAllocator, allocateAndDeallocateAll) {
    ;
    std::vector<std::shared_ptr<Buf64>> ptrs;
    auto &fixedSizeAllocator = FixedSizeAllocator<sizeof(Buf64) + 24>::oneAndOnly();
    for (size_t step = 0; step < 10; step++) {
        for (size_t i = 0; i < 10000; i++) {
            ASSERT_EQ(fixedSizeAllocator.allocatedCount(), i);

            ptrs.emplace_back(std::allocate_shared<Buf64>(StdFixedAllocator<Buf64>::oneAndOnly()));
            for (int k = 0; k < 8; k++) {
                ptrs.back()->data[k] = step * 1000000 + i * 10 + k;
            }
        }
        ASSERT_EQ(fixedSizeAllocator.allocatedCount(), 10000);
        for (size_t i = 0; i < 10000; i++) {
            for (int k = 0; k < 8; k++) {
                ASSERT_EQ(ptrs[i]->data[k], step * 1000000 + i * 10 + k);
            }
            ASSERT_EQ(fixedSizeAllocator.allocatedCount(), 10000 - i);
            ptrs[i] = nullptr;
        }
        ptrs.clear();
        ASSERT_EQ(fixedSizeAllocator.allocatedCount(), 0);
    }
}


struct BufAndSeed {
    int *const data_;
    const int seed_;

    BufAndSeed(int *data, int seed) : data_(data), seed_(seed) {
        for (int k = 0; k < 16; k++) {
            data_[k] = seed_ + k;
        }
    }

    void validate() {
        for (int k = 0; k < 16; k++) {
            ASSERT_EQ(data_[k], seed_ + k);
        }
    }
};

class AllocateXDeallocateY : public testing::TestWithParam<std::tuple<int, int, int>> {
};

INSTANTIATE_TEST_SUITE_P(InstantiationName,
                         AllocateXDeallocateY,
                         testing::Combine(
                                 testing::Values(1, 7, 8, 9, 63, 64, 65, 511, 512, 513, 4095, 4096, 4097, 100001),
                                 testing::Values(1, 2, 3, 4, 5, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 255,
                                                 100000),
                                 testing::Values(1, 10, 100, 1000)));

TEST_P(AllocateXDeallocateY, BackAndForthAllocation) {
    //allocate X and free Y, N times and then free X and allocate Y*N times
    int x = std::get<0>(GetParam());
    int y = std::get<1>(GetParam());
    int n = std::get<2>(GetParam());
    if (x <= y) {
        return;
    }

    auto &allocator = FixedSizeAllocator<64>::oneAndOnly();
    std::vector<BufAndSeed> values;
    for (int step = 0; step < n; step++) {
        for (int i = 0; i < x; i++) {
            values.emplace_back((int *) allocator.alloc(), step * 100 * x + i * 20);
        }
        for (int i = 0; i < y; i++) {
            values.back().validate();
            allocator.free(values.back().data_);
            values.pop_back();
        }
        ASSERT_EQ(allocator.allocatedCount(), (step + 1) * (x - y));
    }
    for (int step = 0; step < n; step++) {
        for (int i = 0; i < y; i++) {
            values.emplace_back((int *) allocator.alloc(), step * 100 * x + i * 20);
        }
        for (int i = 0; i < x; i++) {
            values.back().validate();
            allocator.free(values.back().data_);
            values.pop_back();
        }
        ASSERT_EQ(allocator.allocatedCount(), n * (x - y) - (step + 1) * (x - y));
    }

}

class AllocateRandXDeallocateRandY : public testing::TestWithParam<std::tuple<int, int>> {
};

INSTANTIATE_TEST_SUITE_P(InstantiationName,
                         AllocateRandXDeallocateRandY,
                         testing::Combine(
                                 testing::Values(7, 8, 9, 63, 64, 65, 511, 512, 513, 4095, 4096, 4097, 100001),
                                 testing::Values(1, 10, 100, 1000)));


TEST_P(AllocateRandXDeallocateRandY, BackAndForthAllocation) {
    //allocate X and free Y, N times and then free X and allocate Y*N times
    int x = std::get<0>(GetParam());
    int n = std::get<1>(GetParam());

    auto &allocator = FixedSizeAllocator<64>::oneAndOnly();
    std::vector<BufAndSeed> values;
    size_t totalAllocations = 0;
    for (int step = 0; step < n; step++) {
        int stepUp = std::rand() % x;
        for (int i = 0; i < stepUp; i++) {
            values.emplace_back((int *) allocator.alloc(), step * 100 * x + i * 20);
        }
        totalAllocations += stepUp;
        if (totalAllocations > 0) {
            int stepDown = std::rand() % totalAllocations;
            totalAllocations -= stepDown;
            for (int i = 0; i < stepDown; i++) {
                values.back().validate();
                allocator.free(values.back().data_);
                values.pop_back();
            }
        }
        ASSERT_EQ(allocator.allocatedCount(), totalAllocations);
    }

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

