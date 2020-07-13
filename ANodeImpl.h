#ifndef EXPERIMENTS_ANODEIMPL_H
#define EXPERIMENTS_ANODEIMPL_H

#include "ANodeDecl.h"
#include "BuilderDecl.h"

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
ANode<T, MAX_COUNT, SIZE, ADAPTER>::ANode(const ANode &otherNode) : childrenCount_(otherNode.childrenCount_),
                                                                    origin_(otherNode.origin_),
                                                                    isLeaf_(otherNode.isLeaf_) {
    for (int i = 0; i < childrenCount_; i++) {
        children_[i] = otherNode.children_[i];
        cumSize_[i] = otherNode.cumSize_[i];
        offset_[i] = otherNode.offset_[i];
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
ANode<T, MAX_COUNT, SIZE, ADAPTER>::ANode(ANode &&otherNode) noexcept : childrenCount_(otherNode.childrenCount_),
                                                                        origin_(std::move(otherNode.origin_)),
                                                                        isLeaf_(otherNode.isLeaf_) {
    for (int i = 0; i < childrenCount_; i++) {
        children_[i] = std::move(otherNode.children_[i]);
        cumSize_[i] = otherNode.cumSize_[i];
        offset_[i] = otherNode.offset_[i];
    }
    otherNode.childrenCount_ = 0;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
int8_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::height() const {
    return std::visit([](const auto originPtr) {
        return originPtr->height();
    }, origin_);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::originSize() const {
    return std::visit([](const auto originPtr) {
        return originPtr->size();
    }, origin_);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::maxCompactionSize() const {
    if (!height()) {
        return SIZE / MAX_COUNT;
    }
    return size_t(1) << ((log(SIZE) - 1) + (log(MAX_COUNT) - 1) * (height() - 1));
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::retainedSize() const {
    size_t result = 0;
    for (size_t i = 0; i < childrenCount_; i++) {
        if (!children_[i]) {
            result += childRetainedSize(i);
        }
    }
    return result;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t
ANode<T, MAX_COUNT, SIZE, ADAPTER>::fillLeaf(const ANode::VarType &child, T *destLeaf, size_t offset, size_t length) {
    return std::visit([&](const auto &nodePtr) {
        return nodePtr->fillLeaf(destLeaf, offset, length);
    }, child);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class Visitor>
decltype(auto) ANode<T, MAX_COUNT, SIZE, ADAPTER>::visitChild(Visitor &&_visitor, size_t childPos) const {
    if (!children_[childPos]) {
        return std::visit(std::forward<Visitor>(_visitor), origin_);
    }
    if (isLeaf_[childPos]) {
        return _visitor(static_cast<const LeafT *>(children_[childPos].get()));
    }
    return _visitor(static_cast<const BNodeT *>(children_[childPos].get()));
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::fillLeaf(T *destLeaf, size_t offset, size_t length) const {
    if (offset >= size()) {
        return 0;
    }
    length = std::min(length, size() - offset);
    auto totalLen = length;
    for (size_t childPos = lowerBoundPos(offset + 1); childPos < childrenCount_ && length != 0; childPos++) {
        size_t childOffset = childPos == 0 ? 0 : cumSize_[childPos - 1];
        assert(childOffset <= offset);
        size_t elementsRead = visitChild([&](const auto &childPtr) {
            int localOffset = offset - childOffset;
            return childPtr->fillLeaf(destLeaf, offset_[childPos] + localOffset,
                                      std::min(length, childRetainedSize(childPos) - localOffset));
        }, childPos);
        offset += elementsRead;
        length -= elementsRead;
        destLeaf += elementsRead;
    }
    assert(length == 0);
    return totalLen;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
const T &ANode<T, MAX_COUNT, SIZE, ADAPTER>::operator[](size_t index) const {
    assert(index < size());
    auto childPos = lowerBoundPos(index + 1);
    return visitChild([&](const auto &childPtr) -> const T & {
        return (*childPtr)[offset_[childPos] + index - (childPos ? cumSize_[childPos - 1] : 0)];
    }, childPos);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto ANode<T, MAX_COUNT, SIZE, ADAPTER>::childAt(
        size_t childPos) const -> const std::variant<const LeafT *, const BNodeT *, const VarType> {
    assert(childPos < childrenCount_);
    if (!children_[childPos]) {
        return {origin_};
    }
    if (isLeaf_[childPos]) {
        return {static_cast<const LeafT *>(children_[childPos].get())};
    }
    return {static_cast<const BNodeT *>(children_[childPos].get())};
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::normalizeLength(size_t length, size_t offset, size_t incomingSize) const {
    if (offset > incomingSize) {
        //just in case, in release mode we ignore such usages
        return 0;
    }
    //Normalize length to the max available
    if (length > incomingSize - offset) {
        return incomingSize - offset;
    }
    return length;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
bool ANode<T, MAX_COUNT, SIZE, ADAPTER>::canAcceptNode(const NODE_T &incomingNode, bool asPrefix, size_t offset,
                                                       size_t length) const {
    if constexpr (std::is_same_v<NODE_T, ANodeCPtr> || std::is_same_v<NODE_T, ANodePtr>) {
        return false;
    }
    length = normalizeLength(length, offset, incomingNode->size());
    if (length == 0) {
        return true;
    }
    if (childrenCount_ == MAX_COUNT) {
        const void *pointerToAdd = isOrigin(incomingNode) ? nullptr : static_cast<const void *>(incomingNode.get());
        if (asPrefix) {
            if (pointerToAdd == (children_[0] ? children_[0].get() : nullptr) && offset + length == offset_[0]) {
                return true;
            }
        } else if (
                pointerToAdd == (children_[childrenCount_ - 1] ? children_[childrenCount_ - 1].get() : nullptr) &&
                offset == offset_[childrenCount_ - 1] + cumSize_[childrenCount_ - 1] -
                          (childrenCount_ > 1 ? cumSize_[childrenCount_ - 2] : 0)) {
            return true;
        }
        if (!canCompact()) {
            return false;
        }
    }
    if (isOrigin(incomingNode)) {
        return true;
    }
    if (height()) {
        if (incomingNode->height() < height()) {
            return length >= minChildRetention(incomingNode);
        }
        return false;
    }
    return length <= SIZE * 2 / MAX_COUNT;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::minChildRetention(const NODE_T &incomingNode) {
    constexpr bool isBNode = std::is_same_v<std::remove_cvref_t<NODE_T>, BNodePtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, BNodeCPtr>;

    if constexpr (isBNode) {
        return (incomingNode->size() / incomingNode->childrenCount());
    } else {
        return 1;
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void ANode<T, MAX_COUNT, SIZE, ADAPTER>::shiftNodes(size_t startPos, size_t newCount, int64_t sizeDelta) {
    assert(newCount > childrenCount_);
    for (size_t i = 1; i <= childrenCount_ - startPos; i++) {
        offset_[newCount - i] = offset_[childrenCount_ - i];
        cumSize_[newCount - i] = cumSize_[childrenCount_ - i] + sizeDelta;
        children_[newCount - i] = std::move(children_[childrenCount_ - i]);
        isLeaf_[newCount - i] = isLeaf_[childrenCount_ - i];
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
bool ANode<T, MAX_COUNT, SIZE, ADAPTER>::isOrigin(const NODE_T &incomingNode) const {
    return std::visit([&](const auto &ptr) {
        return static_cast<const void *>(ptr.get());
    }, origin_) == static_cast<const void *>(incomingNode.get());
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void ANode<T, MAX_COUNT, SIZE, ADAPTER>::addNode(NODE_T &&incomingNode, size_t offset, size_t length, bool asPrefix,
                                                 void *context) {
    if constexpr (std::is_same_v<typename ANode<T, MAX_COUNT, SIZE, ADAPTER>::VarType, std::remove_cvref_t<NODE_T>>) {
        addNodeVar(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
    } else if constexpr (std::is_null_pointer_v<NODE_T>) {
        addNode(origin_, offset, length, asPrefix);
        return;
    } else {
        if (!incomingNode) {
            addNode(origin_, offset, length, asPrefix);
            return;
        }
        assert(offset < incomingNode->size());
        if (!incomingNode->isConst()) {
            throw std::logic_error("Can only add const children");
        }
        bool isLeaf;
        if constexpr (std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>) {
            isLeaf = true;
        } else if constexpr (std::is_same_v<std::remove_cvref_t<NODE_T>, BNodeCPtr>) {
            isLeaf = false;
        } else {
            throw std::logic_error("Unsupported type");
        }
        //Normalize length to the max available
        length = normalizeLength(length, offset, incomingNode->size());
        if (!length) {
            //zero size additions need to be ignored
            return;
        }
        assert(canAcceptNode(incomingNode, asPrefix, offset, length));
        GenericCPtr pointerToAdd;
        if (!isOrigin(incomingNode)) {
            pointerToAdd = std::static_pointer_cast<const void>(std::forward<NODE_T>(incomingNode));
        }
        if (childrenCount_) {
            //checking if the added node fits the existing node in place
            if (asPrefix) {
                if (pointerToAdd == children_[0] && offset + length == offset_[0]) {
                    offset_[0] -= length;
                    for (int i = 0; i < childrenCount_; i++) {
                        cumSize_[i] += length;
                    }
                    return;
                }
            } else if (pointerToAdd == children_[childrenCount_ - 1] && offset == offset_[childrenCount_ - 1] +
                                                                                  cumSize_[childrenCount_ - 1] -
                                                                                  (childrenCount_ > 1 ? cumSize_[
                                                                                          childrenCount_ - 2] : 0)) {
                cumSize_[childrenCount_ - 1] += length;
                return;
            }
        }
        if (childrenCount_ == MAX_COUNT) {
            compact(context);
        }
        assert(childrenCount_ < MAX_COUNT);
        size_t destPos = asPrefix ? 0 : childrenCount_;
        if (asPrefix) {
            shiftNodes(0, childrenCount_ + 1, length);
            cumSize_[0] = length;
        } else {
            cumSize_[childrenCount_] = size() + length;
        }
        offset_[destPos] = offset;
        children_[destPos] = std::move(pointerToAdd);
        isLeaf_[destPos] = isLeaf;
        childrenCount_++;
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void ANode<T, MAX_COUNT, SIZE, ADAPTER>::addNodeVar(ANode::VarType &&incomingNode, size_t offset, size_t length,
                                                    bool asPrefix) {
    std::visit([&](auto &&nodePtr) {
        addNode(std::move(nodePtr), offset, length, asPrefix);
    }, std::move(incomingNode));
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void
ANode<T, MAX_COUNT, SIZE, ADAPTER>::addNodeVar(const ANode::VarType &incomingNode, size_t offset, size_t length,
                                               bool asPrefix) {
    std::visit([&](const auto &nodePtr) {
        addNode(nodePtr, offset, length, asPrefix);
    }, incomingNode);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void ANode<T, MAX_COUNT, SIZE, ADAPTER>::removeNodes(uint16_t startPoint, uint16_t count) {
    assert(count <= childrenCount_);
    size_t sizeDelta = cumSize_[startPoint + count - 1] - (startPoint > 0 ? cumSize_[startPoint - 1] : 0);
    for (int i = startPoint; i < childrenCount_ - count; i++) {
        offset_[i] = offset_[i + count];
        cumSize_[i] = cumSize_[i + count] - sizeDelta;
        children_[i] = std::move(children_[i + count]);
        isLeaf_[i] = isLeaf_[i + count];
    }
    childrenCount_ -= count;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool ANode<T, MAX_COUNT, SIZE, ADAPTER>::canCompact() const {
    assert(childrenCount_);
    size_t prevSum = childRetainedSize(0);
    for (uint16_t i = 1; i < childrenCount_; i++) {
        size_t currentSum = childRetainedSize(i);
        if (prevSum + currentSum <= maxCompactionSize()) {
            return true;
        }
        prevSum = currentSum;
    }
    return false;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void ANode<T, MAX_COUNT, SIZE, ADAPTER>::compact(void *context) {
    assert(childrenCount_);
    size_t runningSum = 0;
    uint16_t windowStart = 0;
    for (uint16_t i = 0; i < childrenCount_; i++) {
        size_t currentSum = childRetainedSize(i);
        if (runningSum + currentSum <= maxCompactionSize()) {
            runningSum += currentSum;
        } else {
            if (i - windowStart > 1) {
                compact(windowStart, i - windowStart, context);
                return;
            }
            if (currentSum < maxCompactionSize()) {
                windowStart = i;
                runningSum = currentSum;
            } else {
                windowStart = i + 1;
                runningSum = 0;
            }
        }
    }
    if (childrenCount_ - windowStart > 1) {
        compact(windowStart, childrenCount_ - windowStart, context);
        return;
    }
    throw std::logic_error("Unable to compact (should call canCompactFirst)");
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto ANode<T, MAX_COUNT, SIZE, ADAPTER>::createNodePtr(const ANode &src) -> ANode::ANodePtr {
    static auto &alloc = StdFixedAllocator<ANode>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, src);
    return ANodePtr(p);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto ANode<T, MAX_COUNT, SIZE, ADAPTER>::createNodePtr(ANode &&src) -> ANode::ANodePtr {
    static auto &alloc = StdFixedAllocator<ANode>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, std::move(src));
    return ANodePtr(p);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void ANode<T, MAX_COUNT, SIZE, ADAPTER>::compact(uint16_t fromNode, uint16_t nodeCount, void *context) {
    BuilderT builder(height() - 1);
    builder.setContext(context);
    size_t expectedSize = 0;
    for (uint16_t pos = fromNode; pos < fromNode + nodeCount; pos++) {
        expectedSize += childRetainedSize(pos);
        if (!children_[pos]) {
            builder.addNode(origin_, offset_[pos], childRetainedSize(pos));
        } else if (isLeaf_[pos]) {
            builder.addNode(std::static_pointer_cast<const LeafT>(std::move(children_[pos])), offset_[pos],
                            childRetainedSize(pos));
        } else {
            builder.addNode(std::static_pointer_cast<const BNodeT>(std::move(children_[pos])), offset_[pos],
                            childRetainedSize(pos));
        }
    }
    size_t newSize = builder.size();
    assert(expectedSize == newSize);
    while (builder.isAnode()) {
        builder.pushDownAnnotations();
        newSize = builder.size();
        assert(expectedSize == newSize);
    }
    std::visit([&](auto &&nodePtr) {
        if constexpr (is_unique_ptr_v<decltype(nodePtr)>) {
            throw std::logic_error("Builder must return const nodes");
        } else if constexpr (std::is_same_v<ANodeCPtr, std::remove_cvref_t<decltype(nodePtr)>>) {
            throw std::logic_error("Builder should not return an ANode");
        } else {
            children_[fromNode] = std::static_pointer_cast<const void>(nodePtr);
            isLeaf_.set(fromNode, std::is_same_v<LeafCPtr, std::remove_cvref_t<decltype(nodePtr)>>);
        }
    }, builder.close(false));

    offset_[fromNode] = 0;
    cumSize_[fromNode] = newSize + (fromNode ? cumSize_[fromNode - 1] : 0);
    removeNodes(fromNode + 1, nodeCount - 1);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class Visitor>
void
ANode<T, MAX_COUNT, SIZE, ADAPTER>::forEachChildMove(Visitor &&visitor, size_t offset, size_t length, bool asPrefix) {
    length = std::min(length, size() - offset);
    size_t firstNodePos, lastNodePos;
    std::tie(firstNodePos, lastNodePos) = nodeRangeInclusive(offset, length);
    size_t currentOffset = offset - (firstNodePos ? cumSize_[firstNodePos - 1] : 0);
    auto visitorInternal = [&](auto &&child, bool isLeaf, size_t childOffset, size_t childLen) {
        if (!child) {
            if (origin_.index() == 0) {
                visitor(std::get<LeafCPtr>(origin_), childOffset, childLen);
            } else {
                visitor(std::get<BNodeCPtr>(origin_), childOffset, childLen);
            }
        } else if (isLeaf) {
            visitor(std::static_pointer_cast<const LeafT>(std::move(child)), childOffset, childLen);
        } else {
            visitor(std::static_pointer_cast<const BNodeT>(std::move(child)), childOffset, childLen);
        }
    };

    if (asPrefix) {
        if (firstNodePos != lastNodePos) {
            visitorInternal(std::move(children_[lastNodePos]), isLeaf_[lastNodePos], offset_[lastNodePos],
                            length + offset - cumSize_[lastNodePos - 1]);
            length = cumSize_[lastNodePos - 1] - offset;
            for (auto i = lastNodePos - 1; i > firstNodePos; i--) {
                visitorInternal(std::move(children_[i]), isLeaf_[i], offset_[i], sizeAt(i));
                length -= sizeAt(i);
            }
        }
        visitorInternal(std::move(children_[firstNodePos]), isLeaf_[firstNodePos],
                        offset_[firstNodePos] + currentOffset,
                        length);
    } else {
        for (auto i = firstNodePos; i < lastNodePos; i++) {
            size_t len = sizeAt(i) - currentOffset;
            visitorInternal(std::move(children_[i]), isLeaf_[i], offset_[i] + currentOffset,
                            sizeAt(i) - currentOffset);
            currentOffset = 0;
            length -= len;
        }
        visitorInternal(std::move(children_[lastNodePos]), isLeaf_[lastNodePos], offset_[lastNodePos] + currentOffset,
                        std::min(sizeAt(lastNodePos) - currentOffset, length));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class Visitor>
void
ANode<T, MAX_COUNT, SIZE, ADAPTER>::forEachChild(Visitor &&visitor, size_t offset, size_t length, bool asPrefix) const {
    auto visitorInternal = [&](const auto &child, bool isLeaf, size_t childOffset, size_t childLen) {
        if (!child) {
            if (origin_.index() == 0) {
                visitor(std::get<LeafCPtr>(origin_), childOffset, childLen);
            } else {
                visitor(std::get<BNodeCPtr>(origin_), childOffset, childLen);
            }
        } else if (isLeaf) {
            visitor(std::static_pointer_cast<const LeafT>(child), childOffset, childLen);
        } else {
            visitor(std::static_pointer_cast<const BNodeT>(child), childOffset, childLen);
        }
    };

    length = std::min(length, size() - offset);
    size_t firstNodePos, lastNodePos;
    std::tie(firstNodePos, lastNodePos) = nodeRangeInclusive(offset, length);
    size_t currentOffset = offset - (firstNodePos ? cumSize_[firstNodePos - 1] : 0);
    if (asPrefix) {
        if (firstNodePos != lastNodePos) {
            visitorInternal(children_[lastNodePos], isLeaf_[lastNodePos], offset_[lastNodePos],
                            length + offset - cumSize_[lastNodePos - 1]);
            length = cumSize_[lastNodePos - 1] - offset;
            for (auto i = lastNodePos - 1; i > firstNodePos; i--) {
                visitorInternal(children_[i], isLeaf_[i], offset_[i], sizeAt(i));
                length -= sizeAt(i);
            }
        }
        visitorInternal(children_[firstNodePos], isLeaf_[firstNodePos], offset_[firstNodePos] + currentOffset, length);
    } else {
        for (auto i = firstNodePos; i < lastNodePos; i++) {
            size_t len = sizeAt(i) - currentOffset;
            visitorInternal(children_[i], isLeaf_[i], offset_[i] + currentOffset, len);
            currentOffset = 0;
            length -= len;
        }
        visitorInternal(children_[lastNodePos], isLeaf_[lastNodePos], offset_[lastNodePos] + currentOffset,
                        std::min(sizeAt(lastNodePos) - currentOffset, length));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
std::tuple<size_t, size_t> ANode<T, MAX_COUNT, SIZE, ADAPTER>::nodeRangeInclusive(size_t offset, size_t length) const {
    size_t firstNodePos =
            std::upper_bound(cumSize_.begin(), cumSize_.begin() + childrenCount_, offset) - cumSize_.begin();
    size_t lastNodePos = lowerBoundPos(offset + length);
    return {firstNodePos, lastNodePos};
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
size_t ANode<T, MAX_COUNT, SIZE, ADAPTER>::lowerBoundPos(size_t offset) const {
    return std::lower_bound(cumSize_.begin(), cumSize_.begin() + childrenCount_, offset) - cumSize_.begin();
}


#endif //EXPERIMENTS_ANODEIMPL_H
