#ifndef EXPERIMENTS_BUILDERDECL_H
#define EXPERIMENTS_BUILDERDECL_H

#include "BNode.h"
#include "ANode.h"
#include "Leaf.h"
#include "BuilderFwd.h"
#include <limits>

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
class Builder {
    //Types
public:
    using LeafT = Leaf<T, SIZE, ADAPTER>;
    using BNodeT = BNode<T, MAX_COUNT, SIZE, ADAPTER>;
    using ANodeT = ANode<T, MAX_COUNT, SIZE, ADAPTER>;

    using LeafAllocator = StdFixedAllocator<LeafT>;
    using Allocator = StdFixedAllocator<ANodeT>;
    using BNodeAllocator = StdFixedAllocator<BNodeT>;

    using LeafDeleter = DeleterForFixedAllocator<LeafT>;
    using ANodeDeleter = DeleterForFixedAllocator<ANodeT>;
    using BNodeDeleter = DeleterForFixedAllocator<BNodeT>;

    using LeafPtr = std::unique_ptr<LeafT, LeafDeleter>;
    using LeafCPtr = std::shared_ptr<const LeafT>;
    using BNodePtr = std::unique_ptr<BNodeT, BNodeDeleter>;
    using ANodePtr = std::unique_ptr<ANodeT, ANodeDeleter>;
    using ANodeCPtr = std::shared_ptr<const ANodeT>;
    using BNodeCPtr = std::shared_ptr<const BNodeT>;
    using VarType = typename BNodeT::VarType;
private:
    using ANodeVarType = typename ANodeT::VarType;

    int8_t maxMutationLevel_ = std::numeric_limits<int8_t>::max();
    VarType root_;
    void *context_ = nullptr;

    static constexpr size_t maxHeight() {
        return (64 - log(SIZE / 2)) / log(MAX_COUNT / 2) + (((64 - log(SIZE / 2)) % log(MAX_COUNT / 2)) ? 1 : 0);
    }

    class MutationLevelKeeper {
        Builder &builder_;
        int8_t restoreLevel_;
    public:
        MutationLevelKeeper(const MutationLevelKeeper &) = delete;

        MutationLevelKeeper(MutationLevelKeeper &&) = delete;

        MutationLevelKeeper(Builder &builder, int8_t setLevel) : builder_(builder),
                                                                 restoreLevel_(builder_.maxMutationLevel_) {
            builder_.maxMutationLevel_ = setLevel;
        }

        ~MutationLevelKeeper() {
            builder_.maxMutationLevel_ = restoreLevel_;
        }
    };

    enum Side {
        Back, Front
    };

public:
    Builder() = default;

    explicit Builder(int8_t maxMutationLevel) : maxMutationLevel_(maxMutationLevel) {}

    void pushDownAnnotations();

    bool isAnode() { return BNodeT::isANode(root_); }

    void setContext(void *context) { context_ = context; }

private:

    auto getPeer(const Side side, std::array<BNodeT *, maxHeight()> parents, int8_t targetHeight) -> VarType *;

    /**
     *
     * @tparam NODE_T
     * @param asPrefix
     * @param incomingNode
     * @param offset
     * @param length
     * @return True is it was added and False otherwise
     */
    template<class NODE_T>
    bool addToExisting(const bool asPrefix, NODE_T &&incomingNode, size_t offset, size_t length);

    auto getANodeConst(const VarType &node) -> const ANodeT *;

    auto getBNodeConst(const VarType &node) -> const BNodeT *;

    auto getLeafConst(const VarType &node) -> const LeafT &;

    bool pruneSingleChildRoots(std::array<BNodeT *, maxHeight()> &parents);

    bool pruneSingleChildRoots(std::array<BNodeT *, maxHeight()> &parents, int &pos);

    bool pruneEmptyParentsAndSingleRoots(const Builder<T, MAX_COUNT, SIZE, ADAPTER>::Side &side,
                                         std::array<BNodeT *, maxHeight()> &parents, int8_t targetHeight);

    template<class NODE_PTR>
    void removeNodeAndAddToPeer(const Side side, std::array<BNodeT *, maxHeight()> &parents,
                                VarType &peer, int8_t targetHeight);

    void updateParents(Builder::Side side, std::array<BNodeT *, maxHeight()> &parents, int8_t minHeight = 0);

    template<class NODE>
    void balanceAgainstANode(Builder::Side &side, std::array<BNodeT *, maxHeight()> &parents,
                             const std::unique_ptr<NODE, DeleterForFixedAllocator<NODE>> &currentNode,
                             VarType *peer);
//Contract: parents[height] - the parent of the node of height, i.e. parent[child.height] is parent of child
    /**
     *
     * @param side
     * @param parents
     * @returns <code>true</code> if any balancing actions were taken and <code>false</code> if the sub-tree was left
     * unchanged
     */
    bool balanceLeaf(Side side, std::array<BNodeT *, maxHeight()> &parents);

    bool balanceBNode(Builder::Side side, std::array<BNodeT *, maxHeight()> &parents, BNodePtr &currentNode);

    /**
     *
     * @param path
     * @param parents
     * @param currentNode
     * @return <code>true</code> if any balancing actions were taken and <code>false</code> if the sub-tree was left
     * unchanged
     */
    bool balance(Side side, std::array<BNodeT *, maxHeight()> &parents, int8_t height);

    void balance(Side side);

    void balanceAll();

    void addChildren(VarType &&incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max(),
                     bool asPrefix = false);

    void addChildren(const VarType &incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max(),
                     bool asPrefix = false);

    template<class NODE_T>
    void addChildren(std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &&incomingNode, size_t offset = 0,
                     size_t length = std::numeric_limits<size_t>::max(),
                     bool asPrefix = false);

    template<class NODE_T>
    void addChildren(const std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &incomingNode, size_t offset = 0,
                     size_t length = std::numeric_limits<size_t>::max(),
                     bool asPrefix = false);

    template<class NODE_T>
    void addChildren(std::shared_ptr<const NODE_T> &&incomingNode, size_t offset = 0,
                     size_t length = std::numeric_limits<size_t>::max(),
                     bool asPrefix = false);

    template<class NODE_T>
    void addChildren(const std::shared_ptr<const NODE_T> &incomingNode, size_t offset = 0,
                     size_t length = std::numeric_limits<size_t>::max(),
                     bool asPrefix = false);


    template<class NODE_T>
    static auto annotateNode(NODE_T &&incomingNode, size_t offset = 0,
                             size_t length = std::numeric_limits<size_t>::max()) -> VarType;

public:
    //Utilities

    template<class NODE_T>
    auto slice(NODE_T &&incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max());

    template<class NODE_T>
    void addNode(NODE_T &&incomingNode, size_t offset = 0, size_t length = std::numeric_limits<size_t>::max(),
                 bool asPrefix = false);

    template<class NODE_T>
    void addNode(NODE_T &&incomingNode, bool asPrefix) {
        addNode(std::forward<NODE_T>(incomingNode), 0, std::numeric_limits<size_t>::max(), asPrefix);
    }

    template<class NODE_T>
    void swapAdd(NODE_T &&incomingNode, size_t offset, size_t length, bool asPrefix, bool writeInFull);

    auto close(bool allowAnodeRoot = true) -> VarType;

    size_t size() const { return BNodeT::sizeOf(root_); };

    int8_t height() const { return BNodeT::height(root_); };


    const T &operator[](size_t index) {
        return std::visit([index](const auto &ptr) -> const T & {
            return (*ptr)[index];
        }, root_);
    }

    static void forEachLeaf(auto &&visitor, const auto &node, size_t offset, size_t len);
    static void forEachLeafPtr(auto &&visitor, const auto &node, size_t offset, size_t len);

};

#endif //EXPERIMENTS_BUILDERDECL_H
