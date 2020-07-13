#ifndef EXPERIMENTS_LEAFIMPL_H
#define EXPERIMENTS_LEAFIMPL_H

#include "LeafDecl.h"

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
void Leaf<T, SIZE, ADAPTER>::add(const T *source, size_t length, bool asPrefix /*= false*/) {//TODO update mirror
    assert(length + length_ <= capacity_);
    if (asPrefix) {
        if (length > offset_) {
            Adapter::shiftData(leaf_, offset_, length, length_);
            offset_ = length;
        }
        Adapter::setValues(leaf_, offset_ - length, source, length);
        offset_ -= length;
    } else {
        if (length_ + length + offset_ > capacity_) {
            Adapter::shiftData(leaf_, offset_, 0, length_);
            offset_ = 0;
        }
        Adapter::setValues(leaf_, length_ + offset_, source, length);
    }
    length_ += length;
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
void Leaf<T, SIZE, ADAPTER>::add(const Leaf &src, size_t offset, size_t length, bool asPrefix /*= false*/) {
    offset = std::min(offset, src.length_);
    length = std::min(length, src.length_ - offset);

//    static void copy(DeclaredType &dest, size_t destOffset, const DeclaredType &src, size_t srcOffset, size_t length) {
    if (asPrefix) {
        if (length > offset_) {
            Adapter::shiftData(leaf_, offset_, length, length_);
            offset_ = length;
        }
        Adapter::copy(leaf_, offset_ - length, src.leaf_, offset + src.offset_, length);
        //Adapter::setValues(leaf_, offset_ - length, source, length);
        offset_ -= length;
    } else {
        if (length_ + length + offset_ > capacity_) {
            Adapter::shiftData(leaf_, offset_, 0, length_);
            offset_ = 0;
        }
        Adapter::copy(leaf_, length_ + offset_, src.leaf_, offset + src.offset_, length);
        //Adapter::setValues(leaf_, length_ + offset_, source, length);
    }
    length_ += length;

//    Adapter::copy(leaf_, offset_, src.leaf_, offset + src.offset_, len);
//        add(&Adapter::constArray(src.leaf_)[offset + src.offset_], len, asPrefix);
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
void Leaf<T, SIZE, ADAPTER>::slice(size_t offset, size_t len) {
    assert(offset_ + offset + len <= offset_ + length_);
    offset_ += offset;
    length_ = len;
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
void Leaf<T, SIZE, ADAPTER>::mutate(void* context) {
    Adapter::mutate(leaf_,context);
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
size_t Leaf<T, SIZE, ADAPTER>::available() const {
    if (!isMutable()) {
        return 0;
    }
    return capacity_ - length_ - offset_;
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
void Leaf<T, SIZE, ADAPTER>::makeConst() {
    Adapter::makeConst(leaf_);
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
size_t Leaf<T, SIZE, ADAPTER>::fillLeaf(T *destLeaf, size_t offset, size_t length) const {
    if (offset > length_) {
        return 0;
    };

    length = std::min(length_ - offset, length);
    //DeclaredType &dest, size_t destOffset, const DeclaredType &src, size_t srcOffset, size_t length) {
    Adapter::getValues(destLeaf, leaf_, offset + offset_, length);
    return length;
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
size_t Leaf<T, SIZE, ADAPTER>::setValues(const T *srcLeaf, size_t offset, size_t length) {
    if (offset > length_) {
        return 0;
    };
    length = std::min(length_ - offset, length);
    //(DeclaredType &dest, size_t offset, const T *srcLeaf, size_t length)
    Adapter::setValues(leaf_, offset + offset_, srcLeaf, length);
    return length;
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
auto Leaf<T, SIZE, ADAPTER>::createLeafPtr(const Leaf &src) -> LeafPtr {
    static auto &alloc = StdFixedAllocator<Leaf>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, src);
    return LeafPtr(p);
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
auto Leaf<T, SIZE, ADAPTER>::createLeafPtr(Leaf &&src) -> LeafPtr {
    static auto &alloc = StdFixedAllocator<Leaf>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, std::move(src));
    return LeafPtr(p);
}

template<class T, size_t SIZE, template<class, size_t> class ADAPTER>
auto Leaf<T, SIZE, ADAPTER>::createLeafCPtr(Leaf &&src) -> LeafCPtr {
    static auto &alloc = StdFixedAllocator<Leaf>::oneAndOnly();
    auto p = alloc.allocate(1);
    alloc.construct(p, std::move(src));
    return LeafCPtr(p,LeafDeleter());
}

#endif //EXPERIMENTS_LEAFIMPL_H
