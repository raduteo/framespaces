#ifndef EXPERIMENTS_LEAFDECL_H
#define EXPERIMENTS_LEAFDECL_H

#include "LeafFwd.h"
#include <memory>
#include <variant>
#include "FixedSizeAllocator.h"
#include "FixedSizeArrayAllocator.h"
#include "AllocatorHelpers.h"
#include "ArrayAdapter.h"

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
class Leaf {

    using Adapter = ADAPTER<T, SIZE>;

    using ArrayPtr = typename Adapter::ArrayPtr;
    using ArrayCPtr = typename Adapter::ArrayCPtr;
    using VarType = typename Adapter::DeclaredType;

    // effectively each new Arrow Ref need to point to the source of data,
    // once made const that translation unit gets added to the log

    using LeafDeleter = DeleterForFixedAllocator<Leaf>;
    using LeafPtr = std::unique_ptr<Leaf, LeafDeleter>;
    using LeafCPtr = std::shared_ptr<Leaf>;


    VarType leaf_;
    size_t offset_;
    size_t length_;
    size_t capacity_;



public:
    Leaf(ArrayPtr &&ownerLeaf, size_t offset, size_t length, size_t capacity) :
            leaf_(std::move(ownerLeaf)), offset_(offset), length_(length), capacity_(capacity) {}

    Leaf(ArrayCPtr &&ownerLeaf, size_t offset, size_t length, size_t capacity) :
            leaf_(std::move(ownerLeaf)), offset_(offset), length_(length),
            capacity_(capacity) {}

    Leaf(const ArrayCPtr &ownerLeaf, size_t offset, size_t length, size_t capacity) :
            leaf_(ownerLeaf), offset_(offset), length_(length), capacity_(capacity) {}

    /*
     * Leaf copies involve a deep copy when mutable a pointer copy when constant
     */
    Leaf(const Leaf &srcLeaf) : leaf_(Adapter::copy(srcLeaf.leaf_)),
                                      offset_(srcLeaf.offset_),
                                      length_(srcLeaf.length_),
                                      capacity_(srcLeaf.capacity_) {}

    Leaf(Leaf &&srcLeaf) : leaf_(std::move(srcLeaf.leaf_)),
                                 offset_(srcLeaf.offset_),
                                 length_(srcLeaf.length_),
                                 capacity_(srcLeaf.capacity_) {}

    //explicit Leaf() : Leaf(ArrayPtr(alloc.allocate(1), Deleter()), 0, 0, SIZE) {}

    static Leaf createLeaf(void* context) { return Leaf(Adapter::createLeaf(context), 0, 0, SIZE); }

    void add(const T *source, size_t length, bool asPrefix = false);

    const T at(size_t pos) const { return Adapter::at(leaf_, offset_ + pos); }

    const T operator[](size_t pos) const { return at(pos); }

    void setAt(size_t pos, const T &value) { Adapter::setAt(leaf_, offset_ + pos, value); }

    void
    add(const Leaf &src, size_t offset = 0, size_t len = std::numeric_limits<size_t>::max(), bool asPrefix = false);

    void slice(size_t offset, size_t len);

    void mutate(void* context);

    bool isConst() const { return !Adapter::isMutable(leaf_); }

    size_t available() const;

    void makeConst();

    void makeSeamConst(bool onFront) {};//nothing since the leaf doesn't have seams;

    bool isMutable() const { return Adapter::isMutable(leaf_); }

    bool isNull() { return Adapter::isNull(leaf_); }

    size_t size(bool isConst = false) const { return length_; }

    bool isBalanced() const { return length_ >= SIZE / 2; }

    bool isOneSideBalanced(bool isRoot, bool onFront) const { return isRoot || isBalanced(); }

    bool isDeepBalanced(bool isRoot = false) const { return isRoot || isBalanced(); }

    size_t fillLeaf(T *destLeaf, size_t offset, size_t length) const;

    size_t setValues(const T *srcLeaf, size_t offset, size_t length);

    static auto createLeafPtr(const Leaf &src) -> LeafPtr;

    static auto createLeafPtr(Leaf &&src) -> LeafPtr;

    static auto createLeafCPtr(Leaf &&src) -> LeafCPtr;

    int8_t height() const { return 0; }
};

#endif //EXPERIMENTS_LEAFDECL_H
