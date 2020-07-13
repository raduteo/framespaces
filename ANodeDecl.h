#ifndef EXPERIMENTS_ANODEDECL_H
#define EXPERIMENTS_ANODEDECL_H

#include "utils.h"
#include "Leaf.h"
#include "BNode.h"
#include "ANodeFwd.h"
#include "BuilderFwd.h"
#include  <bitset>

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
class ANode {
    //Types
public:
    using LeafT = Leaf<T, SIZE, ADAPTER>;
    using BNodeT = BNode<T, MAX_COUNT, SIZE, ADAPTER>;
    using BuilderT = Builder<T, MAX_COUNT, SIZE, ADAPTER>;


    using LeafAllocator = StdFixedAllocator<LeafT>;
    using Allocator = StdFixedAllocator<ANode>;
    using BNodeAllocator = StdFixedAllocator<BNodeT>;

    using LeafDeleter = DeleterForFixedAllocator<LeafT>;
    using ANodeDeleter = DeleterForFixedAllocator<ANode>;
    using BNodeDeleter = DeleterForFixedAllocator<BNodeT>;

    using LeafPtr = std::unique_ptr<LeafT, LeafDeleter>;
    using ANodePtr = std::unique_ptr<ANode, ANodeDeleter>;
    using BNodePtr = std::unique_ptr<BNodeT, BNodeDeleter>;
    using LeafCPtr = std::shared_ptr<const LeafT>;
    using ANodeCPtr = std::shared_ptr<const ANode>;
    using BNodeCPtr = std::shared_ptr<const BNodeT>;
    using VarType = std::variant<LeafCPtr, BNodeCPtr>;

private:
    using GenericCPtr = std::shared_ptr<const void>;

    //Members
    std::array<GenericCPtr, MAX_COUNT> children_;
    std::array<size_t, MAX_COUNT> cumSize_;
    std::array<size_t, MAX_COUNT> offset_;
    uint16_t childrenCount_ = 0;
    const VarType origin_;
    std::bitset<MAX_COUNT> isLeaf_;


    static_assert(size_t(1) << log(SIZE) == SIZE);
    static_assert(size_t(1) << log(MAX_COUNT) == MAX_COUNT);


public:
    explicit ANode(const LeafCPtr &origin) : origin_(origin) {};

    explicit ANode(const BNodeCPtr &origin) : origin_(origin) {};

    explicit ANode(LeafCPtr &&origin) : origin_(std::move(origin)) {};

    explicit ANode(BNodeCPtr &&origin) : origin_(std::move(origin)) {};

    explicit ANode(LeafPtr &&origin) : origin_(makeConstFromPtr(std::move(origin), true)) {};

    explicit ANode(BNodePtr &&origin) : origin_(makeConstFromPtr(std::move(origin), true)) {};

    ANode(const ANode &otherNode);


    ANode(ANode &&otherNode) noexcept;

    //Universal node methods
    /**
     * does nothing as the children are already const
     */
    void makeConst() {}

    void mutate(void *context) {}

    void makeSeamConst(bool onFront) {}

    int8_t height() const;

    size_t originSize() const;

    size_t sizeAt(size_t pos) const { return cumSize_[pos] - (pos ? cumSize_[pos - 1] : 0); }

private:

    size_t maxCompactionSize() const;

    size_t childRetainedSize(size_t pos) const { return pos ? cumSize_[pos] - cumSize_[pos - 1] : cumSize_[0]; }

    size_t retainedSize() const;

public:
    size_t minRetention() const { return size_t(1) << ((log(SIZE) - 1) + (log(MAX_COUNT) - 1) * height()); }

    bool isOneSideBalanced(bool isRoot, bool onFront) const { return isRoot || isBalanced(); }

    bool isDeepBalanced(bool isRoot = false) const { return isRoot || isBalanced(); }

    bool isBalanced() const { return retainedSize() >= minRetention(); }

    bool isConst() const { return true; }

    size_t size(bool isConst = false) const { return childrenCount_ ? cumSize_[childrenCount_ - 1] : 0; }

    size_t childrenCount() { return childrenCount_; }

    static size_t fillLeaf(const VarType &child, T *destLeaf, size_t offset, size_t length);

    template<class Visitor>
    decltype(auto) visitChild(Visitor &&_visitor, size_t childPos) const;

    size_t fillLeaf(T *destLeaf, size_t offset, size_t length) const;

    size_t setValues(const T *srcLeaf, size_t offset, size_t length) {
        throw std::logic_error("All data contained by an Node annotation is intrinsically immutable");
    }

    const T &operator[](size_t index) const;

    auto childAt(size_t index) const -> const std::variant<const LeafT *, const BNodeT *, const VarType>;

private:
    size_t normalizeLength(size_t length, size_t offset, size_t incomingSize) const;

public:
    template<class NODE_T>
    bool canAcceptNode(const NODE_T &incomingNode, bool asPrefix = false, size_t offset = 0,
                       size_t length = std::numeric_limits<size_t>::max()) const;

    void shiftNodes(size_t startPos, size_t newCount, int64_t sizeDelta);

    template<class NODE_T>
    bool isOrigin(const NODE_T &incomingNode) const;

    template<class NODE_T>
    void addNode(NODE_T &&incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max(),
                 bool asPrefix = false,void *context = nullptr);

    void addNodeVar(VarType &&incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max(),
                    bool asPrefix = false);

    void addNodeVar(const VarType &incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max(),
                    bool asPrefix = false);

    void removeNodes(uint16_t startPoint, uint16_t count);

private:
    void compact(uint16_t fromNode, uint16_t nodeCount,void* context);

public:
    bool canCompact() const;

    void compact(void* context);

    template<class Visitor>
    void forEachChildMove(Visitor &&visitor, size_t offset, size_t length, bool asPrefix);

    template<class Visitor>
    void forEachChild(Visitor &&visitor, size_t offset, size_t length, bool asPrefix) const;

    std::tuple<size_t, size_t>
    nodeRangeInclusive(size_t offset, size_t length) const;

    size_t lowerBoundPos(size_t offset) const;


    //Utility factories
    static auto createNodePtr(const ANode &src) -> ANodePtr;

    static auto createNodePtr(ANode &&src) -> ANodePtr;

    template<class NODE_T>
    static size_t minChildRetention(const NODE_T &incomingNode);
};

#endif //EXPERIMENTS_ANODEDECL_H
