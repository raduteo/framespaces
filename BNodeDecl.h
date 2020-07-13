#ifndef EXPERIMENTS_BNODEDECL_H
#define EXPERIMENTS_BNODEDECL_H

#include "Leaf.h"
#include "bTraits.h"
#include <type_traits>
#include "ANodeFwd.h"

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
class BNode {
    //Types
    using LeafType = Leaf<T, SIZE, ADAPTER>;
    using ANodeType = ANode<T, MAX_COUNT, SIZE, ADAPTER>;

    using LeafAllocator = StdFixedAllocator<LeafType>;
    using ANodeAllocator = StdFixedAllocator<ANodeType>;

    using LeafDeleter = DeleterForFixedAllocator<LeafType>;
    using ANodeDeleter = DeleterForFixedAllocator<ANodeType>;
    using BNodeDeleter = DeleterForFixedAllocator<BNode>;

    using LeafPtr = std::unique_ptr<LeafType, LeafDeleter>;
    using LeafCPtr = std::shared_ptr<const LeafType>;
    using ANodePtr = std::unique_ptr<ANodeType, ANodeDeleter>;
    using ANodeCPtr = std::shared_ptr<const ANodeType>;
public:
    using Allocator = StdFixedAllocator<BNode>;
    using BNodePtr = std::unique_ptr<BNode, BNodeDeleter>;
    using BNodeCPtr = std::shared_ptr<const BNode>;
    using VarType = std::variant<LeafPtr, ANodePtr, BNodePtr, LeafCPtr, ANodeCPtr, BNodeCPtr>;
private:

    //Members
    std::array<VarType, MAX_COUNT> children_;
    std::array<size_t, MAX_COUNT> cumSize_;
    uint16_t childrenCount_ = 0;
    const int8_t height_;
private:
    //VarType Access

    template<class NODE_T>
    static auto copyNode(const std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &nodePtr) -> VarType;

    template<class NODE_T>
    static VarType copyNode(const std::shared_ptr<const NODE_T> &nodePtr) { return nodePtr; }


    const T &childValueAt(const VarType &node, size_t index) const;

    template<class NODE_T>
    static void makeConstInternal(VarType &node, bool isRoot);

    void shiftNodes(size_t startPos, size_t newCount);

public:

    BNode(int8_t height) : height_(height) {};

    BNode(const BNode &otherNode);

    BNode(BNode &&otherNode) : childrenCount_(otherNode.childrenCount_), cumSize_(std::move(otherNode.cumSize_)),
                               children_(std::move(otherNode.children_)), height_(otherNode.height_) {
        otherNode.childrenCount_ = 0;
    }

    int8_t height() const { return height_; }

    static auto copyNode(const VarType &node) -> VarType;

    template<class NODE>
    static int8_t height(const std::unique_ptr<NODE, DeleterForFixedAllocator<NODE>> &node) { return node->height(); }

    template<class NODE>
    static int8_t height(const std::shared_ptr<NODE> &node) { return node->height(); }

    static int8_t height(const VarType &node);

    VarType &childAt(size_t pos) { return children_[pos]; }

    const VarType &childAt(size_t pos) const { return children_[pos]; }

    size_t childrenCount() const { return childrenCount_; }

    bool isConst() const;

    void mutate(void *context) {};

    void makeConst(bool isRoot = false);

    void makeSeamConst(bool onFront);

    void updateCap(size_t startPos);

    void insertNodes(const std::array<VarType, MAX_COUNT> &srcArray, size_t srcPos, size_t destPos, size_t count);

    void moveNodes(BNode &srcNode, size_t srcPos, size_t destPos, size_t count);

    void addNodes(const BNode &srcNode, size_t srcPos, size_t destPos, size_t count);

    auto removeNode(bool fromFront = false) -> VarType;

    template<class NODE_T>
    void addNode(NODE_T &&incomingNode, bool asPrefix = false);

    auto nodeAt(size_t nodePos) const -> const VarType &;

    auto nodeAt(size_t nodePos) -> VarType &;

    static size_t fillLeaf(const VarType &child, T *destLeaf, size_t offset, size_t length);

    size_t fillLeaf(T *destLeaf, size_t offset, size_t length) const;

    static size_t setValues(const VarType &child, const T *srcLeaf, size_t offset, size_t length);

    size_t setValues(const T *srcLeaf, size_t offset, size_t length);
    //TODO - consider an internal implementation that builds a shared_ptr from the begining

    size_t sizeAt(size_t pos) const { return cumSize_[pos] - (pos ? cumSize_[pos - 1] : 0); }


    //Universal node methods
    size_t size(bool isConst = false) const { return childrenCount_ ? cumSize_[childrenCount_ - 1] : 0; }

    size_t size(bool isConst = false);


    bool isBalanced() const { return childrenCount_ >= MAX_COUNT / 2; }

    bool isOneSideBalanced(bool isRoot, bool onFront) const;

    bool isDeepBalanced(bool isRoot = false) const;

    const T &operator[](size_t index) const;
    //TODO - non-const indexing operation needs a special wrapper object that acts as a

    template<class Visitor>
    void forEachChildMove(Visitor &&visitor, size_t offset, size_t length, bool asPrefix);

    template<class Visitor>
    void forEachChild(Visitor &&visitor, size_t childOffset, size_t childLen, bool asPrefix) const;

    //Utility factories

    static void makeConst(VarType &node, bool isRoot = false);

    static void makeSeamConst(VarType &node, bool onFront);

    static auto createNodePtr(const BNode &src) -> BNodePtr;

    static auto createNodePtr(BNode &&src) -> BNodePtr;

    static size_t sizeOf(const VarType &node);

    static size_t childrenCount(const VarType &node);

    static size_t isConst(const VarType &node);

    static bool isBalanced(const VarType &node);

    static bool isOneSideBalanced(const VarType &node, bool isRoot, bool onFront);

    static size_t isBNode(const VarType &node) { return node.index() == 2 || node.index() == 5; }

    static size_t isANode(const VarType &node) { return node.index() == 1 || node.index() == 4; }

    static size_t isLeaf(const VarType &node) { return node.index() == 0 || node.index() == 3; }

    static int8_t heightOf(const VarType &node);


private:
    //nop - The node is already open
    template<class NODE_T>
    static void
    openInternal(VarType &dest, const std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &nodePtr) {}

    template<class NODE_T>
    static void openInternal(VarType &dest, const std::shared_ptr<const NODE_T> &nodePtr);

public:
    static auto open(VarType &node) -> VarType &;


    std::tuple<size_t, size_t>
    nodeRangeInclusive(size_t offset, size_t length) const;

    size_t lowerBoundPos(size_t offset) const;
};

#endif //EXPERIMENTS_BNODEDECL_H
