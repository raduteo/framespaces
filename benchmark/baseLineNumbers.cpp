#include <benchmark/benchmark.h>


std::pair<std::map<uint64_t,uint64_t>,std::vector<uint64_t>>& getMapForSize(size_t initialCount) {
    static std::map<size_t,std::pair<std::map<uint64_t,uint64_t>,std::vector<uint64_t>>> mapOfMaps;
    const auto &iterator = mapOfMaps.find(initialCount);
    if (iterator==mapOfMaps.end()) {
        std::map<uint64_t, uint64_t> testMap;
        std::vector<uint64_t> keys;
        for (int i = 0; i < initialCount; i++) {
            uint64_t k = (uint64_t(std::rand()) << 32) + std::rand();
            testMap[k] = k * 10;
            keys.push_back(k);
        }
        return mapOfMaps.emplace(
                initialCount, std::pair<std::map<uint64_t, uint64_t>, std::vector<uint64_t>>{
                        std::move(testMap), std::move(keys)}).first->second;
    } else {
        return iterator->second;
    }
}

static void BM_TreeMapGet(benchmark::State &state) {

    size_t initialCount = state.range(0);
    auto& pair = getMapForSize(initialCount);
    std::map<uint64_t, uint64_t> &testMap = pair.first;
    std::vector<uint64_t> &keys = pair.second;
    // Perform setup here
    int pos = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(testMap[keys[pos % keys.size()]]);
        pos++;
    }
}
// Register the function as a benchmark
BENCHMARK(BM_TreeMapGet)->Range(256, 134217728*2);

static void BM_TreeMapAdd(benchmark::State &state) {
    size_t initialCount = state.range(0);
    auto& pair = getMapForSize(initialCount);
    std::map<uint64_t, uint64_t> &testMap = pair.first;
    std::vector<uint64_t> &keys = pair.second;
    // Perform setup here
    int pos = 0;
    for (auto _ : state) {
        testMap[keys[pos]+1] = keys[pos] * 5;
        pos++;
        pos = pos % keys.size();
    }
    benchmark::DoNotOptimize(testMap);
}
// Register the function as a benchmark
BENCHMARK(BM_TreeMapAdd)->Range(256, 134217728*2);
