#ifndef EXPERIMENTS_UTILITIES_H
#define EXPERIMENTS_UTILITIES_H

inline size_t heightForSize(size_t totalSize, size_t MaxCount, size_t Size) {
    size_t nodesCount = totalSize / Size + (totalSize % Size ? 1 : 0);
    size_t result = 1;
    while (nodesCount > 1) {
        result++;
        nodesCount = nodesCount / MaxCount + (nodesCount % MaxCount ? 1 : 0);
    }
    return result;
}

inline size_t sizeForHeight(size_t height, size_t MaxCount, size_t Size) {
    size_t result = Size;
    while (height > 1) {
        result *= MaxCount;
        height--;
    }
    return result;
}

#endif //EXPERIMENTS_UTILITIES_H
