#ifndef EXPERIMENTS_UTILS_H
#define EXPERIMENTS_UTILS_H

#include "AllocatorHelpers.h"

static constexpr size_t log(size_t size) {
    size_t result = 0;
    while (size > 1) {
        size = size / 2;
        result++;
    }
    return result;
}

template<size_t MAX_COUNT, size_t SIZE>
static size_t minSizeForHeight(int8_t height) {
    return size_t(1) << ((log(SIZE) - 1) + (log(MAX_COUNT) - 1) * height);
}


template<class NODE_T>
std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>>
openNode(std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &&node,void* context) {
    return std::move(node);
}

template<class NODE_T>
auto makeConstFromPtr(std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &&ptr, bool isRoot = false) {
    using PtrDeleter = DeleterForFixedAllocator<NODE_T>;
    using SharedPtr = std::shared_ptr<const NODE_T>;
    static auto &allocator = StdFixedAllocator<NODE_T>::oneAndOnly();
    if (!ptr) {
        return SharedPtr();
    }
    if (!ptr->isDeepBalanced(isRoot)) {
        assert(ptr->isDeepBalanced(isRoot));
    }
    ptr->makeConst();
    auto rawPtr = ptr.release();
    return SharedPtr(rawPtr, PtrDeleter(), allocator);
}

template<class NODE_T>
std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> openNode(const std::shared_ptr<const NODE_T> &node,void* context) {
    static auto &alloc = StdFixedAllocator<NODE_T>::oneAndOnly();
    auto pointer = alloc.allocate(1);
    alloc.construct(pointer, *node);
    auto result = std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>>(pointer);
    result->mutate(context);
    return result;
}

template<class NODE_T>
std::shared_ptr<const NODE_T>
closeNode(std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &&node, bool isRoot = false) {
    return makeConstFromPtr(std::move(node), isRoot);
}

template<class NODE_T>
const std::shared_ptr<const NODE_T> closeNode(const std::shared_ptr<const NODE_T> &node, bool isRoot = false) {
    return node;
}

template<class NODE_T>
std::shared_ptr<const NODE_T> closeNode(std::shared_ptr<const NODE_T> &&node) {
    return node;
}

size_t sizeOf(const auto &node) {
    return std::visit([](const auto &nodePtr) -> size_t {
        if (!nodePtr) {
            return 0;
        }
        return nodePtr->size();
    }, node);
}


int8_t heightOf(const auto &node) {
    if constexpr (is_unique_ptr_v<decltype(node)> or is_shared_ptr_v<decltype(node)>) {
        return node->height();
    } else
        return std::visit([](const auto &nodePtr) -> int8_t {
            if (!nodePtr) {
                return 0;
            }
            return nodePtr->height();
        }, node);
}

template<class NODE>
const NODE &getConst(const auto &node) {
    return std::visit([](const auto &nodePtr) -> const NODE & {
        if (!nodePtr) {
            throw std::logic_error("Payload is null");
        }
        if constexpr (std::is_convertible_v<decltype(*nodePtr), const NODE &>) {
            return *nodePtr;
        } else {
            throw std::logic_error("Invalid type");
        }
    }, node);
}

template<class NODE>
bool isDeepBalanced(const NODE &node, bool isRoot = false) {
    return std::visit([&](const auto &nodePtr) {
        return nodePtr->isDeepBalanced(isRoot);
    }, node);
}

auto valueAt(size_t pos, const auto &node) {
    return std::visit([&](const auto &nodePtr) {
        return (*nodePtr)[pos];
    }, node);
}

#endif //EXPERIMENTS_UTILS_H
