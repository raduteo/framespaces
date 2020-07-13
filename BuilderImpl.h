#ifndef EXPERIMENTS_BUILDERIMPL_H
#define EXPERIMENTS_BUILDERIMPL_H

#include "BuilderDecl.h"

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto Builder<T, MAX_COUNT, SIZE, ADAPTER>::getPeer(const Builder::Side side,
                                                   std::array<BNodeT *, maxHeight()> parents,
                                                   int8_t targetHeight) -> Builder::VarType * {
    VarType *peer = nullptr;
    auto rootHeight = BNodeT::heightOf(root_);
    BNodeT *pRoot = std::get<BNodePtr>(root_).get();//Because it must be open and BNodeT if we are digging below it
    //If it encounters an ANodeT then just return it
    //common path is formed of BNodes
    for (int height = rootHeight; height > targetHeight; height--) {
        assert(pRoot);//pRoot is the path to the node whose peer we are looking up so it cannot be null;
        if (pRoot->childrenCount() > 1) {
            peer = &pRoot->childAt(side == Front ? 1 : (pRoot->childrenCount() - 2));
        } else if (peer && BNodeT::isBNode(*peer)) {
            BNodeT::open(*peer);
            auto &bNodePeer = std::get<BNodePtr>(*peer);
            peer = &(bNodePeer->childAt(side == Front ? 0 : (bNodePeer->childrenCount() - 1)));
        }
        auto &rootCandidate = pRoot->childAt(side == Front ? 0 : (pRoot->childrenCount() - 1));
        if (BNodeT::isBNode(rootCandidate)) {
            pRoot = std::get<BNodePtr>(rootCandidate).get();
        } else {
            //if pRoot is not a BNodeT it means we reached the end
            assert(height == targetHeight + 1);
        }
    }
    return peer;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
bool Builder<T, MAX_COUNT, SIZE, ADAPTER>::addToExisting(const bool asPrefix, NODE_T &&incomingNode, size_t offset,
                                                         size_t length) {
    constexpr bool isConst = std::is_same_v<std::remove_cvref_t<NODE_T>, ANodeCPtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, BNodeCPtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>;

    std::array<BNodeT *, maxHeight()> parents;
    BNodeT::open(root_);
    VarType *lastOpenParent = &root_;
    VarType *rollingParent = lastOpenParent;
    int rootHeight = BNodeT::heightOf(root_);
    int currentHeight = rootHeight;
    parents[rootHeight] = nullptr;
    while (currentHeight > incomingNode->height() && rollingParent->index() == 2) {
        BNodePtr &parentAsBNode = std::get<BNodePtr>(*rollingParent);
        parents[currentHeight - 1] = parentAsBNode.get();
        rollingParent = &parentAsBNode->childAt(asPrefix ? 0 : parentAsBNode->childrenCount() - 1);
        currentHeight--;
        if (!BNodeT::isConst(*rollingParent)) {
            lastOpenParent = rollingParent;
        }
    }
    for (int i = currentHeight - 1; i >= 0; i--) {
        parents[i] = nullptr;
    }
    if (pruneSingleChildRoots(parents)) {
        return false;
    }
    if (rootHeight && balance(asPrefix ? Front : Back, parents, rootHeight - 1)) {
        return false;
    }
    if (BNodeT::isANode(*lastOpenParent)) {
        ANodePtr &parentANode = std::get<ANodePtr>(*lastOpenParent);
        if (parentANode->height() <= maxMutationLevel_ &&
            parentANode->canAcceptNode(incomingNode, asPrefix, offset, length)) {
            if (!isConst && !incomingNode->isDeepBalanced(true)) {
                addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
            } else {
                parentANode->addNode(closeNode(std::forward<NODE_T>(incomingNode), true), offset, length, asPrefix,
                                     context_);
            }
            return true;
        } else if (parentANode->isBalanced()) {
            if (parents[parentANode->height()]) {
                parents[parentANode->height()]->size();//update size
            }
            BNodeT::makeConst(*lastOpenParent);
            //then back up
        } else {
            int height = parentANode->height();
            MutationLevelKeeper mutationLevelKeeper(*this, height - 1);
            {
                this->addNode(rootHeight > height ?
                              parents[height]->removeNode(asPrefix) : std::move(root_), 0,
                              std::numeric_limits<size_t>::max(), asPrefix);
            }
            return false;//retry from top level
        }
    }
    BNodeT *lastOpenParentPtr;
    do {
        currentHeight++;
        lastOpenParentPtr = currentHeight <= rootHeight ? parents[currentHeight - 1] : nullptr;
    } while (lastOpenParentPtr && lastOpenParentPtr->childrenCount() == MAX_COUNT);
    if (currentHeight > rootHeight) {
        assert(currentHeight == rootHeight + 1);
        BNodePtr newNode = BNodeT::createNodePtr(BNodeT(currentHeight));
        lastOpenParentPtr = newNode.get();
        parents[rootHeight] = lastOpenParentPtr;
        newNode->addNode(std::move(root_), asPrefix);
        root_ = VarType(std::move(newNode));
    }
    assert(lastOpenParentPtr->childrenCount() < MAX_COUNT);
    while (lastOpenParentPtr->height() > incomingNode->height() + 1) {
        BNodePtr newNode = BNodeT::createNodePtr(BNodeT(lastOpenParentPtr->height() - 1));
        BNodeT *nextParent = newNode.get();
        lastOpenParentPtr->addNode(std::move(newNode), asPrefix);
        lastOpenParentPtr = nextParent;
    }
    if (offset == 0 && length == incomingNode->size()) {
        lastOpenParentPtr->addNode(std::forward<NODE_T>(incomingNode), asPrefix);
    } else {
        lastOpenParentPtr->addNode(annotateNode(std::forward<NODE_T>(incomingNode), offset, length), asPrefix);
    }
    return true;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto Builder<T, MAX_COUNT, SIZE, ADAPTER>::getANodeConst(const Builder::VarType &node) -> const ANodeT * {
    switch (node.index()) {
        case 1:
            return std::get<ANodePtr>(node).get();
        case 4:
            return std::get<ANodeCPtr>(node).get();
        default:
            throw std::logic_error("Not an ANodeT");
    }
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto Builder<T, MAX_COUNT, SIZE, ADAPTER>::getBNodeConst(const Builder::VarType &node) -> const BNodeT * {
    switch (node.index()) {
        case 2:
            return std::get<BNodePtr>(node).get();
        case 5:
            return std::get<BNodeCPtr>(node).get();
        default:
            throw std::logic_error("Not a BNodeT");
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto Builder<T, MAX_COUNT, SIZE, ADAPTER>::getLeafConst(const Builder::VarType &node) -> const LeafT & {
    switch (node.index()) {
        case 0:
            return *std::get<LeafPtr>(node);
        case 3:
            return *std::get<LeafCPtr>(node);
        default:
            throw std::logic_error("Not LeafT");
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::updateParents(Builder::Side side, std::array<BNodeT *, maxHeight()> &parents,
                                                         int8_t minHeight/* = 0*/) {
    parents.fill(nullptr);
    VarType *currentNode = &root_;
    for (auto height = BNodeT::heightOf(root_) - 1; BNodeT::isBNode(*currentNode) && height >= minHeight; height--) {
        BNodeT::open(*currentNode);
        BNodePtr &bNodeCurrent = std::get<BNodePtr>(*currentNode);
        parents[height] = bNodeCurrent.get();
        currentNode = &bNodeCurrent->childAt(side == Front ? 0 : bNodeCurrent->childrenCount() - 1);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::balanceAgainstANode(Builder::Side &side,
                                                               std::array<BNodeT *, maxHeight()> &parents,
                                                               const std::unique_ptr<NODE, DeleterForFixedAllocator<NODE>> &currentNode,
                                                               VarType *peer) {
    const ANodeT *aNodePeer = this->getANodeConst(*peer);
    if (aNodePeer->canAcceptNode(currentNode, side == Front)) {
        this->removeNodeAndAddToPeer<std::unique_ptr<NODE, DeleterForFixedAllocator<NODE>>>(side, parents, *peer,
                                                                                            currentNode->height());
        return;
    }
    auto currentHeight = currentNode->height();
    VarType removedCurrentNode = parents[currentHeight]->removeNode(side == Front);
    int8_t peerHeight = aNodePeer->height();
    pruneEmptyParentsAndSingleRoots(side, parents, currentHeight);
    MutationLevelKeeper mutationLevelKeeper(*this, peerHeight - 1);
    {
        updateParents(side, parents, peerHeight);
        VarType peerNode =
                peerHeight < heightOf(root_) ? parents[peerHeight]->removeNode(side == Front) : std::move(root_);
        pruneEmptyParentsAndSingleRoots(side, parents, peerHeight);
        if (!size()) {
            root_ = VarType();
        }
        addNode(std::move(peerNode), 0, std::numeric_limits<size_t>::max(), side == Front);
        addNode(std::move(removedCurrentNode), 0, std::numeric_limits<size_t>::max(), side == Front);
        updateParents(side, parents, currentHeight);
        balance(side, parents, currentHeight);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool Builder<T, MAX_COUNT, SIZE, ADAPTER>::balanceLeaf(Builder::Side side, std::array<BNodeT *, maxHeight()> &parents) {
    if (BNodeT::isLeaf(root_)) {
        return false;
    }
    VarType &node = parents[0]->childAt(side == Front ? 0 : parents[0]->childrenCount() - 1);
    if (BNodeT::isBalanced(node) || BNodeT::isBalanced(node)) {
        return false;
    }
    LeafPtr &currentNode = std::get<LeafPtr>(node);
    auto peer = getPeer(side, parents, 0);
    if (BNodeT::isANode(*peer)) {
        balanceAgainstANode(side, parents, currentNode, peer);
        return true;
    } //else if peer is a LeafT
    BNodeT::open(*peer);
    LeafPtr &leafPeer = std::get<LeafPtr>(*peer);
    if (leafPeer->size() + currentNode->size() >= SIZE) {
        size_t transferSize = SIZE / 2 - currentNode->size();
        if (side == Front) {
            currentNode->add(*leafPeer, 0, transferSize);
            leafPeer->slice(transferSize, leafPeer->size() - transferSize);
        } else {
            currentNode->add(*leafPeer, leafPeer->size() - transferSize, transferSize, true);
            leafPeer->slice(0, leafPeer->size() - transferSize);
        }
    } else {
        if (side == Front) {
            if (currentNode->available() >= leafPeer->size()) {
                currentNode->add(*leafPeer);
                *peer = std::move(currentNode);
            } else {
                auto newLeaf = LeafT::createLeafPtr(LeafT::createLeaf(context_));
                newLeaf->add(*currentNode);
                newLeaf->add(*leafPeer);
                *peer = std::move(newLeaf);
            }
            parents[0]->removeNode(true);
        } else {
            if (leafPeer->available() >= currentNode->size()) {
                leafPeer->add(*currentNode);
            } else {
                LeafT newLeaf = LeafT::createLeaf(context_);
                newLeaf.add(*leafPeer);
                newLeaf.add(*currentNode);
                *peer = LeafT::createLeafPtr(std::move(newLeaf));
            }
            parents[0]->removeNode();
        }
    }
    pruneEmptyParentsAndSingleRoots(side, parents, 0);
    return true;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool Builder<T, MAX_COUNT, SIZE, ADAPTER>::balanceBNode(Builder::Side side, std::array<BNodeT *, maxHeight()> &parents,
                                                        BNodePtr &currentNode) {
    if (currentNode->isBalanced()) {
        return false;
    }
    int8_t currentHeight = currentNode->height();
    auto peer = getPeer(side, parents, currentNode->height());
    if (BNodeT::isANode(*peer)) {
        balanceAgainstANode(side, parents, currentNode, peer);
        return true;
    } //else if peer is a BNodeT
    BNodeT::open(*peer);
    BNodePtr &bNodePeer = std::get<BNodePtr>(*peer);
    if (bNodePeer->childrenCount() + currentNode->childrenCount() >= MAX_COUNT) {
        size_t transferCount = MAX_COUNT / 2 - currentNode->childrenCount();
        if (side == Front) {
            currentNode->moveNodes(*bNodePeer, 0, currentNode->childrenCount(), transferCount);
        } else {
            currentNode->moveNodes(*bNodePeer, bNodePeer->childrenCount() - transferCount, 0, transferCount);
        }
    } else {
        if (side == Front) {
            bNodePeer->moveNodes(*currentNode, 0, 0, currentNode->childrenCount());
            // parents[currentNode->height()]->removeNode(true);//todo check this removes (newly added) - prune should just fix it all
        } else {
            bNodePeer->moveNodes(*currentNode, 0, bNodePeer->childrenCount(), currentNode->childrenCount());
            // parents[currentNode->height()]->removeNode(false);//todo check this removes (newly added)- prune should just fix it all
        }
    }
    pruneEmptyParentsAndSingleRoots(side, parents, currentHeight - 1);
    return true;
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool Builder<T, MAX_COUNT, SIZE, ADAPTER>::pruneEmptyParentsAndSingleRoots(
        const Builder<T, MAX_COUNT, SIZE, ADAPTER>::Side &side,
        std::array<BNodeT *, maxHeight()> &parents,
        int8_t targetHeight) {
    bool treeChanged = false;
    for (int8_t pos = targetHeight + 1; pos < height() && !parents[pos - 1]->childrenCount(); pos++) {
        parents[pos - 1] = nullptr;
        parents[pos]->removeNode(side == Front);
        treeChanged = true;
    }
    int pos;
    treeChanged = pruneSingleChildRoots(parents, pos) || treeChanged;
    if (pos >= 0 && BNodeT::isBNode(root_)) {
        parents[pos] = std::get<BNodePtr>(root_).get();
    }
    while (pos > 0) {
        if (parents[pos]) {
            auto &candidate = parents[pos]->childAt(side == Front ? 0 : (parents[pos]->childrenCount() - 1));
            if (candidate.index() == 2) {
                parents[pos - 1] = std::get<BNodePtr>(candidate).get();
            }
        } else {
            parents[pos - 1] = nullptr;
        }
        pos--;
    }
    return treeChanged;
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_PTR>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::removeNodeAndAddToPeer(const Side side,
                                                                  std::array<BNodeT *, maxHeight()> &parents,
                                                                  VarType &peer, int8_t targetHeight) {
    BNodeT::open(peer);
    ANodeT *peerPtr = std::get<ANodePtr>(peer).get();
    VarType removedNode = parents[targetHeight]->removeNode(side == Front);
    peerPtr->addNode(closeNode(std::get<NODE_PTR>(std::move(removedNode)), true), 0,
                     std::numeric_limits<size_t>::max(), side == Front,context_);
    pruneEmptyParentsAndSingleRoots(side, parents, targetHeight);
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool
Builder<T, MAX_COUNT, SIZE, ADAPTER>::balance(Builder::Side side, std::array<BNodeT *, maxHeight()> &parents,
                                              int8_t height) {
    bool treeChanged = false;
    while (true) {
        int8_t rootHeight = heightOf(root_);
        if (rootHeight < height) {
            return treeChanged;
        }
        if (rootHeight > height && !parents[height]) {
            return treeChanged;
        }
        auto &currentNode = (rootHeight == height ? root_ :
                             parents[height]->childAt(side == Front ? 0 : parents[height]->childrenCount() - 1));
        switch (currentNode.index()) {
            case 0: {
                return balanceLeaf(side, parents) || treeChanged;
            }
            case 1: {
                if (BNodeT::isBalanced(currentNode)) {
                    return treeChanged; // all ANodes subnodes are always balanced
                }
                ANodePtr &aNodePtr = std::get<ANodePtr>(currentNode);
                int height = aNodePtr->height();
                MutationLevelKeeper mutationLevelKeeper(*this, height - 1);
                {
                    VarType nodeToApplyMutations =
                            height < rootHeight ? parents[height]->removeNode(side == Front) : std::move(root_);
                    pruneEmptyParentsAndSingleRoots(side, parents, height);
                    this->addNode(std::move(nodeToApplyMutations), 0, std::numeric_limits<size_t>::max(),
                                  side == Front);
                    this->updateParents(side, parents, height);
                    this->balance(side, parents, height);
                }
                return true;
            }
            case 2: {
                BNodePtr &bNodePtr = std::get<BNodePtr>(currentNode);
                int height = bNodePtr->height();
                parents[height - 1] = bNodePtr.get();
                if (balance(side, parents, height - 1)) {
                    treeChanged = true;
                    continue; //if anything happened below we try again in case the current node changed
                }
                if (height == rootHeight) {
                    return treeChanged;
                }
                return balanceBNode(side, parents, bNodePtr) || treeChanged;
            }

                //BNodeT
                // if peer can accept BNodeT children, remove node from parent and add children to peer
                // if peer ANodeT and canCompact - compact and if peer still balanced retry else (if peer not balanced) - apply peer mutations and add
                // else if peer ANodeT - apply peer mutation (remove peer, node, add their children with mutation ceil at
                //          height and then balance at height
                // else borrow min number of peer children
            default:
                return treeChanged;//All set if const
        }
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool Builder<T, MAX_COUNT, SIZE, ADAPTER>::pruneSingleChildRoots(std::array<BNodeT *, maxHeight()> &parents) {
    int pos;
    return pruneSingleChildRoots(parents, pos);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
bool Builder<T, MAX_COUNT, SIZE, ADAPTER>::pruneSingleChildRoots(std::array<BNodeT *, maxHeight()> &parents, int &pos) {
    bool treeChanged;
    for (pos = heightOf(root_) - 1;
         BNodeT::isBNode(root_) && std::get<BNodePtr>(root_)->childrenCount() == 1; pos--) {
        parents[pos] = nullptr;
        VarType newRoot = std::move(std::get<BNodePtr>(root_)->childAt(0));
        root_ = std::move(newRoot);
        if (BNodeT::isBNode(root_)) {
            BNodeT::open(root_);
            parents[pos - 1] = std::get<BNodePtr>(root_).get();
        } else if (pos > 0) {
            parents[pos - 1] = nullptr;
        }
        treeChanged = true;
    }
    return treeChanged;
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void
Builder<T, MAX_COUNT, SIZE, ADAPTER>::addChildren(Builder::VarType &&incomingNode, size_t offset, size_t length,
                                                  bool asPrefix) {
    std::visit([&](auto &&nodePtr) {
        addChildren(std::move(nodePtr), offset, length, asPrefix);
    }, std::move(incomingNode));
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void
Builder<T, MAX_COUNT, SIZE, ADAPTER>::addChildren(const Builder::VarType &incomingNode, size_t offset, size_t length,
                                                  bool asPrefix) {
    std::visit([&](const auto &nodePtr) {
        addNode(nodePtr, offset, length, asPrefix);
    }, incomingNode);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::addChildren(
        std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &&incomingNode,
        size_t offset /*= 0*/,
        size_t length /*= std::numeric_limits<size_t>::max()*/,
        bool asPrefix /*= false*/) {
    if constexpr (std::is_same_v<NODE_T, LeafT>) {
        throw std::logic_error("LeafT has no children");
    } else {
        incomingNode->forEachChildMove([&](auto &&child, size_t childOffset, size_t childLength) {
            addNode(std::forward<decltype(child)>(child), childOffset, childLength, asPrefix);
        }, offset, length, asPrefix);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void
Builder<T, MAX_COUNT, SIZE, ADAPTER>::addChildren(
        const std::unique_ptr<NODE_T, DeleterForFixedAllocator<NODE_T>> &incomingNode,
        size_t offset /*= 0*/,
        size_t length /*= std::numeric_limits<size_t>::max()*/,
        bool asPrefix /*= false*/) {
    if constexpr (std::is_same_v<NODE_T, LeafT>) {
        throw std::logic_error("LeafT has no children");
    } else {
        incomingNode->forEachChild([&](const VarType &child, size_t childOffset, size_t childLength) {
            addNode(child, childOffset, childLength, asPrefix);
        }, offset, length, asPrefix);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void
Builder<T, MAX_COUNT, SIZE, ADAPTER>::addChildren(std::shared_ptr<const NODE_T> &&incomingNode, size_t offset /* = 0 */,
                                                  size_t length /* = std::numeric_limits<size_t>::max() */,
                                                  bool asPrefix /*= false */) {

    addChildren(incomingNode, offset, length, asPrefix);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::addChildren(const std::shared_ptr<const NODE_T> &incomingNode,
                                                       size_t offset /*= 0*/,
                                                       size_t length /*= std::numeric_limits<size_t>::max()*/,
                                                       bool asPrefix /*= false*/) {
    if constexpr (std::is_same_v<NODE_T, LeafT>) {
        throw std::logic_error("LeafT has no children");
    } else {
        incomingNode->forEachChild([&](const auto &child, size_t childOffset, size_t childLength) {
            addNode(child, childOffset, childLength, asPrefix);
        }, offset, length, asPrefix);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
auto Builder<T, MAX_COUNT, SIZE, ADAPTER>::slice(NODE_T &&incomingNode, size_t offset, size_t length) {
    constexpr bool isLeaf = std::is_same_v<std::remove_cvref_t<NODE_T>, LeafPtr> or
                            std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>;
    static_assert(isLeaf);
    constexpr bool isConst = std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>;
    if constexpr (isConst) {
        auto result = LeafT::createLeafPtr(LeafT::createLeaf(context_));
        result->add(*incomingNode, offset, length);
        return result;
    } else {
        incomingNode->slice(offset, length);
        return std::forward<NODE_T>(incomingNode);
    }
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::addNode(NODE_T &&incomingNode, size_t offset, size_t length, bool asPrefix) {
    constexpr bool isANode = std::is_same_v<std::remove_cvref_t<NODE_T>, ANodePtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, ANodeCPtr>;
    constexpr bool isBNode = std::is_same_v<std::remove_cvref_t<NODE_T>, BNodePtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, BNodeCPtr>;
    constexpr bool isLeaf = std::is_same_v<std::remove_cvref_t<NODE_T>, LeafPtr> or
                            std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>;
    constexpr bool isConst = std::is_same_v<std::remove_cvref_t<NODE_T>, ANodeCPtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, BNodeCPtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>;
    if constexpr (std::is_same_v<std::remove_cvref_t<NODE_T>, VarType>) {
        std::visit([&](auto &&nodePtr) {
            if constexpr (is_unique_ptr_v<std::remove_cvref_t<decltype(nodePtr)>>) {
                addNode(std::move(nodePtr), offset, length, asPrefix);
            } else {
                addNode(std::forward<decltype(nodePtr)>(nodePtr), offset, length, asPrefix);
            }
        }, std::forward<NODE_T>(incomingNode));
    } else if constexpr (std::is_same_v<std::remove_cvref_t<NODE_T>, ANodeVarType>) {
        switch (incomingNode.index()) {
            case 0:
                addNode(std::get<0>(incomingNode), offset, length, asPrefix);
                break;
            case 1:
                addNode(std::get<1>(incomingNode), offset, length, asPrefix);
                break;
            default:
                throw std::logic_error("There can only be two ...");
        }
    } else if constexpr (std::is_const_v<std::remove_reference_t<NODE_T>> && !isConst) {
        throw std::logic_error("Cannot insert a const unique pointer");
    } else {
        if (!incomingNode) {
            return;
        }
        length = std::min(length, incomingNode->size() - offset);
        if (!length) {
            return;
        }
        bool writeInFull = offset == 0 && incomingNode->size() == length;

        while (true) {
            int8_t incomingHeight = incomingNode->height();
            if (incomingHeight > maxMutationLevel_) {
                if constexpr (isANode) {
                    addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
                    return;
                }
                if (!writeInFull) {
                    if constexpr (isLeaf) {
                        addNode(slice(std::forward<NODE_T>(incomingNode), offset, length), asPrefix);
                    } else {
                        addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
                    }
                    return;
                }
            }
            if (!writeInFull && (isANode || !(isConst || incomingNode->isDeepBalanced() || isLeaf))) {
                addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
                return;
            }
            if (!sizeOf(root_)) {
                if (writeInFull) {
                    root_ = VarType(std::forward<NODE_T>(incomingNode));
                } else if (!incomingHeight || isConst || incomingNode->isBalanced()) {
                    root_ = annotateNode(std::forward<NODE_T>(incomingNode), offset, length);
                }
                return;
            }
            //if incoming not balanced on the inner seam then addChildren
            if (!incomingNode->isOneSideBalanced(true, !asPrefix)) {
                addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
                return;
            }
            if (incomingHeight >= BNodeT::heightOf(root_)) {
                if (incomingHeight && length < minSizeForHeight<MAX_COUNT, SIZE>(incomingHeight)) {
                    addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
                    return;
                }
                if (incomingHeight > heightOf(root_)) {
                    if (!writeInFull && !incomingNode->isDeepBalanced()) {
                        //i.e. is we cannot mutate the incoming node then write children
                        addChildren(std::forward<NODE_T>(incomingNode), offset, length, asPrefix);
                        return;
                    }
                    swapAdd(std::forward<NODE_T>(incomingNode), offset, length, asPrefix, writeInFull);
                    return;
                }
                //heights are equal
                if (!BNodeT::isOneSideBalanced(root_, true, asPrefix)) {
                    //if seam not balanced then flip the addition
                    swapAdd(std::forward<NODE_T>(incomingNode), offset, length, asPrefix, writeInFull);
                    return;
                }
                if constexpr (isBNode) {
                    if (writeInFull && BNodeT::isBNode(root_)) {
                        size_t rootChildrenCount = getBNodeConst(root_)->childrenCount();
                        if (BNodeT::isBNode(root_) && incomingNode->childrenCount() + rootChildrenCount <= MAX_COUNT) {
                            if (!BNodeT::isConst(root_) || std::is_same_v<std::remove_cvref_t<NODE_T>, BNodeCPtr>) {
                                BNodeT::open(root_);
                                if constexpr (std::is_same_v<std::remove_cvref_t<NODE_T>, BNodePtr>) {
                                    std::get<BNodePtr>(root_)->moveNodes(*incomingNode, 0,
                                                                         asPrefix ? 0 : rootChildrenCount,
                                                                         incomingNode->childrenCount());
                                } else {
                                    std::get<BNodePtr>(root_)->addNodes(*incomingNode, 0,
                                                                        asPrefix ? 0 : rootChildrenCount,
                                                                        incomingNode->childrenCount());
                                }
                            } else {
                                if constexpr (std::is_same_v<std::remove_cvref_t<NODE_T>, BNodePtr>) {
                                    auto openedIncoming = openNode(std::forward<NODE_T>(incomingNode), context_);
                                    openedIncoming->addNodes(*std::get<BNodeCPtr>(root_), 0,
                                                             asPrefix ? openedIncoming->childrenCount() : 0,
                                                             rootChildrenCount);
                                    root_ = std::forward<NODE_T>(openedIncoming);
                                } else {
                                    throw std::logic_error("This should be unreachable");
                                }
                            }
                            return;
                        }
                    }
                }
                if constexpr (isANode) {
                    if (BNodeT::isBalanced(root_) && std::visit([&](auto &&rootPtr) {
                        if (incomingNode->canAcceptNode(rootPtr, !asPrefix)) {
                            auto openedIncoming = openNode(std::forward<NODE_T>(incomingNode), context_);
                            if constexpr (is_unique_ptr_v<decltype(rootPtr)>) {
                                openedIncoming->addNode(makeConstFromPtr(std::move(rootPtr), true), 0,
                                                        std::numeric_limits<size_t>::max(), !asPrefix,context_);
                            } else {
                                openedIncoming->addNode(std::move(rootPtr), 0, std::numeric_limits<size_t>::max(),
                                                        !asPrefix,context_);
                            }
                            root_ = std::move(openedIncoming);
                            return true;
                        }
                        return false;
                    }, std::move(root_))) {
                        return;
                    }
                }
                if (BNodeT::isANode(root_)) {
                    if (getANodeConst(root_)->canAcceptNode(incomingNode, asPrefix, offset, length)) {
                        BNodeT::open(root_);
                        std::get<ANodePtr>(root_)->addNode(closeNode(std::forward<NODE_T>(incomingNode), true),
                                                           offset, length, asPrefix, context_);
                        return;
                    }
                }
                if constexpr (isLeaf) {
                    if (BNodeT::isLeaf(root_) && BNodeT::sizeOf(root_) + length <= SIZE) {
                        const LeafT &rootConstLeaf = getLeafConst(root_);
                        if (asPrefix) {
                            if constexpr (!isConst) {
                                if (!writeInFull) {
                                    incomingNode->slice(offset, length);
                                    writeInFull = true;
                                    offset = 0;
                                }
                                if (incomingNode->available() >= rootConstLeaf.size()) {
                                    incomingNode->add(rootConstLeaf);
                                    root_ = std::move(incomingNode);
                                    return;
                                }
                            }
                            LeafT newLeaf = LeafT::createLeaf(context_);
                            newLeaf.add(*incomingNode, offset, length);
                            newLeaf.add(rootConstLeaf);
                            root_ = VarType(LeafT::createLeafPtr(std::move(newLeaf)));
                        } else {
                            if (rootConstLeaf.available() >= length) {
                                BNodeT::open(root_);
                                std::get<LeafPtr>(root_)->add(*incomingNode, offset, length);
                            } else {
                                LeafT newLeaf = LeafT::createLeaf(context_);
                                newLeaf.add(rootConstLeaf);
                                newLeaf.add(*incomingNode, offset, length);
                                root_ = VarType(LeafT::createLeafPtr(std::move(newLeaf)));
                            }
                        }
                        return;
                    }
                }
                //add a common root
                if (writeInFull) {
                    if constexpr (!isConst) {
                        incomingNode->makeSeamConst(!asPrefix);
                    }
                    if (!BNodeT::isConst(root_)) {
                        BNodeT::makeSeamConst(root_, asPrefix);
                    }
                }
                VarType newNode = writeInFull ? isConst && !incomingNode->isBalanced() ?
                                                VarType(openNode(std::forward<NODE_T>(incomingNode), context_)) :
                                                VarType(std::forward<NODE_T>(incomingNode)) :
                                  VarType(annotateNode(std::forward<NODE_T>(incomingNode), offset, length));
                std::visit([&](auto &&rootPtr) {
                    BNodePtr newRoot = BNodeT::createNodePtr(BNodeT(rootPtr->height() + 1));
                    if (asPrefix) {
                        newRoot->addNode(std::move(newNode));
                        if (is_shared_ptr_v<std::remove_cvref_t<decltype(rootPtr)>>) {
                            newRoot->addNode(openNode(std::move(rootPtr), context_));
                        } else {
                            newRoot->addNode(std::move(rootPtr));
                        }
                    } else {
                        if (is_shared_ptr_v<std::remove_cvref_t<decltype(rootPtr)>>) {
                            newRoot->addNode(openNode(std::move(rootPtr), context_));
                        } else {
                            newRoot->addNode(std::move(rootPtr));
                        }
                        newRoot->addNode(std::move(newNode));
                    }
                    root_ = std::move(newRoot);
                }, std::move(root_));
                return;
            }
            if (isConst && !incomingNode->isBalanced()) {
                addNode(openNode(std::forward<NODE_T>(incomingNode), context_), offset, length, asPrefix);
                return;
            } else if (addToExisting(asPrefix, std::forward<NODE_T>(incomingNode), offset, length)) {
                return;
            }
        }
    }
}


template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::swapAdd(NODE_T &&incomingNode, size_t offset, size_t length, bool asPrefix,
                                                   bool writeInFull) {
    VarType oldNode = std::move(root_);
    if (!writeInFull) {
        root_ = annotateNode(std::forward<NODE_T>(incomingNode), offset, length);
    } else {
        root_ = VarType(std::forward<NODE_T>(incomingNode));
    }
    addNode(oldNode, 0, std::numeric_limits<size_t>::max(), !asPrefix);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
template<class NODE_T>
auto
Builder<T, MAX_COUNT, SIZE, ADAPTER>::annotateNode(NODE_T &&incomingNode, size_t offset, size_t length) -> VarType {
    constexpr bool isANode = std::is_same_v<std::remove_cvref_t<NODE_T>, ANodePtr> or
                             std::is_same_v<std::remove_cvref_t<NODE_T>, ANodeCPtr>;
    constexpr bool isLeaf = std::is_same_v<std::remove_cvref_t<NODE_T>, LeafPtr> or
                            std::is_same_v<std::remove_cvref_t<NODE_T>, LeafCPtr>;
    if constexpr (isANode) {
        throw std::logic_error("Cannot annotate an already annotated node");
    } else {
        if constexpr (is_unique_ptr_v<NODE_T>) {
            if constexpr (isLeaf) {
                incomingNode->slice(offset, length);
                return VarType(std::move(incomingNode));
            }
            return annotateNode(makeConstFromPtr(std::move(incomingNode)), offset, length);
        }
        auto newRoot = ANodeT::createNodePtr(ANodeT(std::forward<NODE_T>(incomingNode)));
        newRoot->addNode(nullptr, offset, length);
        return VarType(std::move(newRoot));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
auto Builder<T, MAX_COUNT, SIZE, ADAPTER>::close(bool allowAnodeRoot) -> VarType {
    do {
        auto originalSize = size();
        if (BNodeT::isANode(root_) && !BNodeT::isBalanced(root_)) {
            Builder builder(heightOf(root_) - 1);
            builder.setContext(context_);
            builder.addNode(std::move(root_));
            assert(originalSize == builder.size());
            return builder.close();
        }
        balanceAll();
        if (sizeOf(root_)) {
            BNodeT::makeConst(root_, true);
        }
        if (allowAnodeRoot || !isAnode()) {
            break;
        }
        pushDownAnnotations();
    } while (true);
    return std::move(root_);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::pushDownAnnotations() {
    if (!BNodeT::isANode(root_)) {
        return;
    }
    Builder builder(BNodeT::heightOf(root_) - 1);
    builder.setContext(context_);
    builder.addNode(root_);
    root_ = std::move(builder.root_);
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::balance(Builder::Side side) {
    std::array<BNodeT *, maxHeight()> parents{nullptr};
    auto rootHeight = BNodeT::heightOf(root_);
    if (rootHeight == 0) {
        return;
    }
    if (!BNodeT::isConst(root_) && BNodeT::isBNode(root_)) {
        parents[rootHeight - 1] = std::get<BNodePtr>(root_);
        balance(side, parents, rootHeight - 1);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::balanceAll() {
    int8_t balancedHeight;
    bool repeat = false;
    do {
        repeat = false;
        balancedHeight = height();
        std::array<BNodeT *, maxHeight()> parents{nullptr};
        while (BNodeT::isBNode(root_) && getBNodeConst(root_)->childrenCount() == 1) {
            if (BNodeT::isConst(root_)) {
                root_ = BNodeT::copyNode(std::get<BNodeCPtr>(root_)->childAt(0));
            } else {
                auto v = std::move(std::get<BNodePtr>(root_)->childAt(0));
                root_ = std::move(v);
            }
        }
        auto rootHeight = BNodeT::heightOf(root_);
        if (rootHeight == 0) {
            return;
        }
        if (!BNodeT::isConst(root_) && BNodeT::isBNode(root_)) {
            size_t sizeB4 = sizeOf(root_);
            parents[rootHeight - 1] = std::get<BNodePtr>(root_).get();
            balance(Side::Front, parents, rootHeight - 1);
            size_t sizeAfter = sizeOf(root_);
            assert(sizeB4 == sizeAfter);
        }
        rootHeight = BNodeT::heightOf(root_);
        if (rootHeight == 0) {
            return;
        }
        balancedHeight = std::min(balancedHeight, rootHeight);
        if (!BNodeT::isConst(root_) && BNodeT::isBNode(root_)) {
            parents[rootHeight - 1] = std::get<BNodePtr>(root_).get();
            balance(Side::Back, parents, rootHeight - 1);
            if (!BNodeT::isOneSideBalanced(root_, true, true)) {
                repeat = true;
                if (maxMutationLevel_ > -1) {
                    maxMutationLevel_--;
                }
            }
        }
    } while (height() > balancedHeight || repeat);

}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void
Builder<T, MAX_COUNT, SIZE, ADAPTER>::forEachLeafPtr(auto &&visitor, const auto &nodePtr, size_t offset, size_t len) {
    using NodeType = decltype(nodePtr);
    constexpr bool isLeaf = std::is_same_v<std::remove_cvref_t<NodeType>, LeafPtr> or
                            std::is_same_v<std::remove_cvref_t<NodeType>, LeafCPtr>;
    if constexpr (isLeaf) {
        size_t localLen = std::min(len, nodePtr->size() - offset);
        visitor(*nodePtr, offset, localLen);
    } else {
        nodePtr->forEachChild([&](const auto &child, size_t childOffset, size_t childLength) {
            forEachLeaf(visitor, child, childOffset, childLength);
        }, offset, len, false);
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE, template<class, size_t> class ADAPTER>
void Builder<T, MAX_COUNT, SIZE, ADAPTER>::forEachLeaf(auto &&visitor, const auto &node, size_t offset, size_t len) {
    if constexpr (is_unique_ptr_v<decltype(node)> || is_shared_ptr_v<decltype(node)>) {
        forEachLeafPtr(visitor, node, offset, len);
    } else {
        std::visit([&](const auto &nodePtr) {
            forEachLeafPtr(visitor, nodePtr, offset, len);
        }, node);
    }
}

#endif //EXPERIMENTS_BUILDERIMPL_H
