#ifndef EXPERIMENTS_BNODEIMPL_H
#define EXPERIMENTS_BNODEIMPL_H

#include "BNodeDecl.h"
#include "utils.h"

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::copyNode(
        const std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &nodePtr) -> BNode::VarType {
    static auto &alloc = StdFixedAllocator<NODE_T>::oneAndOnly();
    auto pointer = alloc.allocate(1);
    alloc.construct(pointer, *nodePtr);
    return std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>>(pointer);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::copyNode(const BNode::VarType &node) -> BNode::VarType {
    return std::visit([](const auto &nodePtr) {
        return copyNode(nodePtr);
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
const T &BNode<T, MAX_COUNT, SIZE, ADAPTER>::childValueAt(const BNode::VarType &node, size_t index) const {
    return std::visit([&](const auto &nodePtr) -> const T & {
        return std::as_const(*nodePtr)[index];
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::makeConstInternal(BNode::VarType &node, bool isRoot) {
    using PtrDeleter = DeleterForFixedAllocator<NODE_T>;
    using UniquePtr = std::unique_ptr<NODE_T, PtrDeleter>;
    std::get<UniquePtr>(node)->size(false);//updating size structures
    node = VarType(makeConstFromPtr(std::get<UniquePtr>(std::move(node)), isRoot));
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::makeConst(BNode::VarType &node, bool isRoot) {
    switch (node.index()) {
        case 0:
            makeConstInternal<LeafType>(node, isRoot);
            break;
        case 1:
            makeConstInternal<ANodeType>(node, isRoot);
            break;
        case 2:
            makeConstInternal<BNode>(node, isRoot);
            break;
        case 3:
        case 4:
        case 5:
            break;
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::makeSeamConst(BNode::VarType &node, bool onFront) {
    std::visit([&](auto &&nodePtr) {
        if constexpr (is_unique_ptr_v<decltype(nodePtr)>) {
            nodePtr->makeSeamConst(onFront);
        }
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::shiftNodes(size_t startPos, size_t newCount) {
    for (size_t i = 1; i <= childrenCount_ - startPos; i++) {
        children_[newCount - i] = std::move(children_[childrenCount_ - i]);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
BNode<T, MAX_COUNT, SIZE, ADAPTER>::BNode(const BNode &otherNode) : childrenCount_(otherNode.childrenCount_),
                                                           height_(otherNode.height_) {
    for (int i = 0; i < childrenCount_; i++) {
        children_[i] = copyNode(otherNode.children_[i]);
        cumSize_[i] = otherNode.cumSize_[i];
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
int8_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::height(const BNode::VarType &node) {
    return std::visit([&](const auto &nodePtr) ->int8_t {
        if (!nodePtr) {
            return 0;
        }
        return nodePtr->height();
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool BNode<T, MAX_COUNT, SIZE, ADAPTER>::isConst() const {
    for (int i = 0; i < childrenCount_; i++) {
        if (children_[i].index() < 3) {
            return false;
        }
    }
    return true;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::makeConst(bool isRoot) {
    int rollingDelta = 0;
    for (int i = 0; i < childrenCount_; i++) {
        if (!isConst(children_[i])) {
            makeConst(children_[i]);
            rollingDelta = (i > 0 ? cumSize_[i - 1] : 0) + sizeOf(children_[i]) - cumSize_[i];
        }
        if (rollingDelta) {
            cumSize_[i] += rollingDelta;
        }
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::updateCap(size_t startPos) {
    auto prevSize = startPos == 0 ? 0 : cumSize_[startPos - 1];
    for (size_t i = startPos; i < childrenCount_; i++) {
        prevSize = cumSize_[i] = sizeOf(children_[i]) + prevSize;
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void
BNode<T, MAX_COUNT, SIZE, ADAPTER>::insertNodes(const std::array<VarType, MAX_COUNT> &srcArray, size_t srcPos, size_t destPos,
                                       size_t count) {
    assert(count + childrenCount_ <= SIZE);
    size_t newCount = childrenCount_ + count;
    shiftNodes(destPos, newCount);
    for (size_t i = 0; i < count; i++) {
        children_[destPos + i] = copyNode(srcArray[srcPos + i]);
    }
    childrenCount_ = newCount;
    updateCap(destPos);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::moveNodes(BNode &srcNode, size_t srcPos, size_t destPos, size_t count) {
    assert(count + childrenCount_ <= SIZE);
    size_t newCount = childrenCount_ + count;
    shiftNodes(destPos, newCount);
    for (int i = 0; i < count; i++) {
        children_[destPos + i] = std::move(srcNode.children_[srcPos + i]);
    }
    size_t cumSrcSize = srcPos ? srcNode.cumSize_[srcPos - 1] : 0;
    for (int i = 0; i < srcNode.childrenCount_ - srcPos - count; i++) {
        srcNode.children_[srcPos + i] = std::move(srcNode.children_[srcPos + i + count]);
        cumSrcSize = srcNode.cumSize_[srcPos + i] = cumSrcSize + sizeOf(srcNode.children_[srcPos + i]);
    }
    srcNode.childrenCount_ -= count;
    childrenCount_ = newCount;
    updateCap(destPos);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::addNodes(const BNode &srcNode, size_t srcPos, size_t destPos, size_t count) {
    assert(count + childrenCount_ <= SIZE);
    size_t newCount = childrenCount_ + count;
    shiftNodes(destPos, newCount);
    for (int i = 0; i < count; i++) {
        children_[destPos + i] = copyNode(srcNode.children_[srcPos + i]);
    }
    childrenCount_ = newCount;
    updateCap(destPos);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::removeNode(bool fromFront) -> BNode::VarType {
    assert(childrenCount_);
    VarType removedNode = std::move(children_[fromFront ? 0 : childrenCount_ - 1]);
    childrenCount_--;
    if (fromFront) {
        size_t sizeDelta = cumSize_[0];
        for (int i = 0; i < childrenCount_; i++) {
            cumSize_[i] = cumSize_[i + 1] - sizeDelta;
            children_[i] = std::move(children_[i + 1]);
        }
    }
    return removedNode;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::addNode(NODE_T &&incomingNode, bool asPrefix) {

    if constexpr (is_shared_ptr_v<NODE_T>) {
        if (!incomingNode->isBalanced()) {
            assert(incomingNode->isBalanced());
        }
    } else if constexpr(not is_unique_ptr_v<NODE_T>) {
        assert(!isConst(incomingNode) || isBalanced(incomingNode));
    }
    assert(childrenCount_ < MAX_COUNT);
    assert(height(incomingNode) == height_ - 1);
    size_t destPos = asPrefix ? 0 : childrenCount_;
    if (asPrefix) {
        shiftNodes(0, childrenCount_ + 1);
    }

    if constexpr (std::is_rvalue_reference<decltype(incomingNode)>::value) {
        children_[destPos] = std::move(incomingNode);
    } else {
        children_[destPos] = copyNode(incomingNode);
    }
    childrenCount_++;
    updateCap(destPos);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::nodeAt(size_t nodePos) const -> const BNode::VarType & {
    assert(nodePos < childrenCount_);
    return children_[nodePos];
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::nodeAt(size_t nodePos) -> VarType & {
    assert(nodePos < childrenCount_);
    return children_[nodePos];
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::fillLeaf(const BNode::VarType &child, T *destLeaf, size_t offset, size_t length) {
    return std::visit([&](const auto &nodePtr) {
        return nodePtr->fillLeaf(destLeaf, offset, length);
    }, child);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::fillLeaf(T *destLeaf, size_t offset, size_t length) const {
    if (offset >= size()) {
        return 0;
    }
    length = std::min(length, size() - offset);
    auto totalLen = length;
    for (size_t childPos = lowerBoundPos(offset + 1);
         childPos < childrenCount_ && length != 0; childPos++) {
        size_t childOffset = childPos == 0 ? 0 : cumSize_[childPos - 1];
        assert(childOffset <= offset);
        size_t elementsRead = fillLeaf(children_[childPos], destLeaf, offset - childOffset, length);
        offset += elementsRead;
        length -= elementsRead;
        destLeaf += elementsRead;
    }
    assert(length == 0);
    return totalLen;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t
BNode<T, MAX_COUNT, SIZE, ADAPTER>::setValues(const BNode::VarType &child, const T *srcLeaf, size_t offset, size_t length) {
    return std::visit([&](const auto &nodePtr) -> size_t {
        std::remove_cvref_t<decltype(nodePtr)> x;
        if constexpr (is_unique_ptr_v<decltype(nodePtr)>) {
            return nodePtr->setValues(srcLeaf, offset, length);
        }
        throw std::logic_error("Cannot set values on a const node");
    }, child);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::setValues(const T *srcLeaf, size_t offset, size_t length) {
    if (offset >= size()) {
        return 0;
    }
    length = std::min(length, size() - offset);
    auto totalLen = length;
    for (size_t childPos = lowerBoundPos(offset + 1);
         childPos < childrenCount_ && length != 0; childPos++) {
        size_t childOffset = childPos == 0 ? 0 : cumSize_[childPos - 1];
        assert(childOffset <= offset);
        size_t elementsRead = setValues(children_[childPos], srcLeaf, offset - childOffset, length);
        offset += elementsRead;
        length -= elementsRead;
        srcLeaf += elementsRead;
    }
    assert(length == 0);
    return totalLen;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
const T &BNode<T, MAX_COUNT, SIZE, ADAPTER>::operator[](size_t index) const {
    assert(index < size());
    auto childPos = lowerBoundPos(index + 1);
    if (childPos) {
        return childValueAt(children_[childPos], index - cumSize_[childPos - 1]);
    } else {
        return childValueAt(children_[childPos], index);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::createNodePtr(const BNode &src) -> BNode::BNodePtr {
    static auto &alloc = StdFixedAllocator<BNode>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, src);
    return BNodePtr(p);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::createNodePtr(BNode &&src) -> BNode::BNodePtr {
    static auto &alloc = StdFixedAllocator<BNode>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, std::move(src));
    return BNodePtr(p);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::sizeOf(const BNode::VarType &node) {
    return std::visit([](const auto &nodePtr) -> size_t {
        if (!nodePtr) {
            return 0;
        }
        return nodePtr->size();
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::childrenCount(const BNode::VarType &node) {
    return std::visit([](const auto &nodePtr) -> size_t {
        if constexpr (std::is_same_v<std::remove_const_t<decltype(*nodePtr)>,Leaf>) {
            return 0;
        }
        if (!nodePtr) {
            return 0;
        }
        return nodePtr->childrenCount();
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::isConst(const BNode::VarType &node) {
    return node.index() > 2;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool BNode<T, MAX_COUNT, SIZE, ADAPTER>::isBalanced(const BNode::VarType &node) {
    return std::visit([](const auto &nodePtr) {
        if (!nodePtr) {
            return false;
        }
        return nodePtr->isBalanced();
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool BNode<T, MAX_COUNT, SIZE, ADAPTER>::isOneSideBalanced(const VarType &node, bool isRoot, bool onFront) {
    return isConst(node) || std::visit([&](const auto &nodePtr) {
        return nodePtr->isOneSideBalanced(isRoot, onFront);
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
int8_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::heightOf(const BNode::VarType &node) {
    return std::visit([](const auto &nodePtr) -> int8_t {
        if (!nodePtr) {
            return 0;
        }
        return nodePtr->height();
    }, node);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::openInternal(BNode::VarType &dest, const std::shared_ptr<const NODE_T> &nodePtr) {
    static auto &alloc = StdFixedAllocator<NODE_T>::oneAndOnly();
    auto pointer = alloc.allocate(1);
    alloc.construct(pointer, *nodePtr);
    dest = std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>>(pointer);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto BNode<T, MAX_COUNT, SIZE, ADAPTER>::open(BNode::VarType &node) -> VarType & {
    std::visit([&](const auto &nodePtr) {
        openInternal(node, nodePtr);
    }, node);
    return node;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool BNode<T, MAX_COUNT, SIZE, ADAPTER>::isOneSideBalanced(bool isRoot, bool onFront) const {
    return (isRoot || isBalanced() ) &&
           BNode::isOneSideBalanced(childAt(onFront ? 0 : childrenCount() - 1), false, onFront);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool BNode<T, MAX_COUNT, SIZE, ADAPTER>::isDeepBalanced(bool isRoot /*= false*/) const {
    if (!isRoot && !isBalanced()) {
        return false;
    }
    for (int i = 0; i < childrenCount_; i++) {
        if (!BNode::isConst(children_[i]) && !::isDeepBalanced(children_[i])) {
            return false;
        }
    }
    return true;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::makeSeamConst(bool onFront) {
    size(false);//updating sizes
    VarType &childToClose = childAt(onFront ? 0 : childrenCount() - 1);
    if (!BNode::isConst(childToClose)) {
        BNode::makeSeamConst(childToClose, onFront);
    }
    if (childrenCount_ > 1) {
        BNode::makeConst(childToClose);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class Visitor>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::forEachChild(Visitor &&visitor, size_t offset, size_t length, bool asPrefix) const {
    length = std::min(length, size() - offset);
    auto visitorInternal = [&](const auto &child, size_t childOffset, size_t childLen) {
        std::visit([&](const auto &ptr) {
            visitor(ptr, childOffset, childLen);
        }, child);
    };
    size_t firstNodePos, lastNodePos;
    std::tie(firstNodePos, lastNodePos) = nodeRangeInclusive(offset, length);
    size_t currentOffset = offset - (firstNodePos?cumSize_[firstNodePos - 1]:0);
    if (asPrefix) {
        if (firstNodePos != lastNodePos) {
            visitorInternal(children_[lastNodePos], 0, length + offset - cumSize_[lastNodePos - 1]);
            length = cumSize_[lastNodePos - 1] - offset;
            for (auto i = lastNodePos - 1; i > firstNodePos; i--) {
                visitorInternal(children_[i], 0, sizeAt(i));
                length -= sizeAt(i);
            }
        }
        visitorInternal(children_[firstNodePos], currentOffset, length);
    } else {
        for (auto i = firstNodePos; i < lastNodePos; i++) {
            size_t len = sizeAt(i) - currentOffset;
            visitorInternal(children_[i], currentOffset, len);
            currentOffset = 0;
            length -= len;
        }
        visitorInternal(children_[lastNodePos], currentOffset, std::min(sizeAt(lastNodePos) - currentOffset, length));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class Visitor>
void BNode<T, MAX_COUNT, SIZE, ADAPTER>::forEachChildMove(Visitor &&visitor, size_t offset, size_t length, bool asPrefix) {
    length = std::min(length, size() - offset);
    size_t firstNodePos;
    size_t lastNodePos;
    std::tie(firstNodePos, lastNodePos) = nodeRangeInclusive(offset, length);
    size_t currentOffset = offset - (firstNodePos?cumSize_[firstNodePos - 1]:0);
    if (asPrefix) {
        if (firstNodePos != lastNodePos) {
            visitor(std::move(children_[lastNodePos]), 0, length + offset - cumSize_[lastNodePos - 1]);
            length = cumSize_[lastNodePos - 1] - offset;
            for (auto i = lastNodePos - 1; i > firstNodePos; i--) {
                visitor(std::move(children_[i]), 0, sizeAt(i));
                length -= sizeAt(i);
            }
        }
        visitor(std::move(children_[firstNodePos]), currentOffset, length);
    } else {
        for (auto i = firstNodePos; i < lastNodePos; i++) {
            size_t len = sizeAt(i) - currentOffset;
            visitor(std::move(children_[i]), currentOffset, len);
            currentOffset = 0;
            length -= len;
        }
        visitor(std::move(children_[lastNodePos]), currentOffset,
                std::min(sizeAt(lastNodePos) - currentOffset, length));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
std::tuple<size_t, size_t>
BNode<T, MAX_COUNT, SIZE, ADAPTER>::nodeRangeInclusive(size_t offset, size_t length) const {
    size_t firstNodePos =
            std::upper_bound(cumSize_.begin(), cumSize_.begin() + childrenCount_, offset) - cumSize_.begin();
    size_t lastNodePos = lowerBoundPos(offset + length);
    return {firstNodePos, lastNodePos};
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::lowerBoundPos(size_t offset) const {
    return std::lower_bound(cumSize_.begin(), cumSize_.begin() + childrenCount_, offset) - cumSize_.begin();
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t BNode<T, MAX_COUNT, SIZE, ADAPTER>::size(bool isConst) {
    if (!childrenCount_) {
        return 0;
    }
    int64_t rollingDelta = 0;
    for (int i = 0; i < childrenCount_; i++) {
        if (!BNode::isConst(children_[i])) {
            rollingDelta = (sizeOf(children_[i]) - sizeAt(i));
        }
        if (rollingDelta) {
            cumSize_[i] += rollingDelta;
        }
    }
    return cumSize_[childrenCount_ - 1];
}

#endif //EXPERIMENTS_BNODEIMPL_H
