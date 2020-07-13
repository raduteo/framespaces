#ifndef EXPERIMENTS_TESTDATATOOL_H
#define EXPERIMENTS_TESTDATATOOL_H

#include "../Builder.h"
#include <array>

struct SkipStepException {
    const std::string message_;

    SkipStepException(std::string message) : message_(message) {}
};

inline size_t longRand() {
    return size_t((size_t(std::rand()) << size_t(32)) + std::rand());
}

class StateIdType {
    using StateVect = std::vector<StateIdType>;
    using Pair = std::pair<size_t, std::shared_ptr<StateIdType>>;
    using VarTypeT = std::variant<size_t, StateVect *, Pair>;
    VarTypeT actualValue;
public:
    explicit StateIdType(size_t v) {
        actualValue = {v};
    }

    explicit StateIdType(std::vector<StateIdType> *v) {
        actualValue = {v};
    };

    StateIdType(size_t val, const StateIdType &remaining) {
        actualValue = std::make_pair(val, std::make_shared<StateIdType>(remaining));
    };


    size_t asSingle() const {
        return std::get<size_t>(actualValue);
    }

    Pair asPair() const {
        return std::get<Pair>(actualValue);
    }

    StateVect *asVect() const {
        return std::get<StateVect *>(actualValue);
    }

    static StateIdType parse(const std::string &src) {
        int pos = 0;
        StateIdType result = parse(src, pos);
        assert(pos == src.size());
        return result;
    }

    static StateIdType parse(const std::string &src, int &pos) {
        switch (src[pos]) {
            case '(': {
                pos++;
                size_t nextV = 0;
                while (src[pos] <= '9' && src[pos] >= '0') {
                    nextV = 10 * nextV + (src[pos] - '0');
                    pos++;
                }
                if (src[pos++] != ',') {
                    throw std::logic_error("Unexpected char");
                }
                StateIdType nextState = parse(src, pos);
                if (src[pos++] != ')') {
                    throw std::logic_error("Unexpected char");
                }
                return StateIdType(nextV, nextState);
            }
            case '{': {
                StateVect result;
                while (src[pos++] != '}') {
                    result.push_back(parse(src, pos));
                    if (src[pos] != '}' && src[pos] != ',') {
                        throw std::logic_error("Unexpected char");
                    }
                }
                return StateIdType(new StateVect(result));
            }
            default: {
                size_t nextV = 0;
                while (src[pos] <= '9' && src[pos] >= '0') {
                    nextV = 10 * nextV + (src[pos] - '0');
                    pos++;
                }
                return StateIdType(nextV);
            }

        }
    }

    friend std::ostream &operator<<(std::ostream &os, const StateIdType &bar) {
        switch (bar.actualValue.index()) {
            case 0: {
                return os << std::get<0>(bar.actualValue);
            }
            case 1: {
                StateVect &vect = *std::get<1>(bar.actualValue);
                os << "{";
                bool first = true;
                for (const auto &item : vect) {
                    if (first) {
                        first = false;
                    } else {
                        os << ",";
                    }
                    os << item;
                }
                return os << "}";
            }
            case 2: {
                const Pair &p = std::get<2>(bar.actualValue);
                return os << "(" << p.first << "," << *p.second << ")";
            }

        }
        throw std::logic_error("Unhandled state");
    }
};

struct Generator {
    /**
    * @return False is it resets back to state and True otherwise
    */
    virtual bool next() = 0;

    virtual void randomNext() = 0;

    virtual StateIdType getStateId() const = 0;

    virtual void setStateId(const StateIdType &stateId) = 0;

    virtual ~Generator() {};
};

template<class VALUE, class DATA>
class TestDataGenerator : public Generator {
public:
    using DataType = DATA;
    using StateType = VALUE;

    virtual StateType getState(DATA &rollingValue) const = 0;
};

template<class VALUE, class DATA>
class LambdaBasedGeneration : public TestDataGenerator<VALUE, DATA> {
    std::function<VALUE(size_t, DATA &)> generator_;
    size_t statesCount_;
    size_t currentStateId_ = 0;

public:
    inline static size_t maxId = 10000;
    size_t id = maxId++;


    LambdaBasedGeneration(std::function<VALUE(size_t, DATA &)> generator,
                          size_t statesCount) : generator_(generator), statesCount_(statesCount) {
        // std::cout << id << std::endl;
    }


    LambdaBasedGeneration(const LambdaBasedGeneration &src) : generator_(src.generator_),
                                                              statesCount_(src.statesCount_),
                                                              currentStateId_(src.currentStateId_) {
        //  std::cout << id << std::endl;
    }

    LambdaBasedGeneration(LambdaBasedGeneration &&src) = delete;

    bool next() override {
        currentStateId_++;
        currentStateId_ %= statesCount_;
        return (currentStateId_ != 0);
    }

    void randomNext() override {
        currentStateId_ = longRand() % statesCount_;
    }

    VALUE getState(DATA &rollingValue) const override {
        return generator_(currentStateId_, rollingValue);
    };

    StateIdType getStateId() const override {
        return StateIdType{currentStateId_};
    }

    void setStateId(const StateIdType &stateId) override {
        currentStateId_ = stateId.asSingle();
    };

};

template<size_t ...I>
void tupleToArray(auto &tuple, auto &array, std::index_sequence<I...>) {
    (void) ((array[I] = static_cast<Generator *>(&std::get<I>(tuple)), 0)+...);
}

template<class VALUE, class DATA>
class ComposedGenerator : public TestDataGenerator<VALUE, DATA> {

    struct StatefulComposer {
        virtual bool next() = 0;

        virtual void randomNext() = 0;

        virtual VALUE getState(DATA &data) const = 0;

        virtual StateIdType getStateId() const = 0;

        virtual void setStateId(const StateIdType &stateId) = 0;

        virtual ~StatefulComposer() {};

        virtual std::unique_ptr<StatefulComposer> clone() const = 0;
    };

    template<class... GENERATORS>
    class StatefulComposerT : public StatefulComposer {
        using Composer = std::function<VALUE(size_t, typename std::remove_cvref_t<GENERATORS>::StateType...)>;
        Composer composer_;
        std::tuple<std::remove_cvref_t<GENERATORS>...> generators_;
        std::vector<Generator *> generatorsVector_;
        mutable std::vector<StateIdType> subStates = {};
        const size_t statesCount_;
        size_t currentState = 0;
    public:
        StatefulComposerT(Composer &&composer, size_t stateCount, GENERATORS &&... generators) :
                composer_(std::forward<Composer>(composer)),
                statesCount_(stateCount),
                generators_(std::forward<GENERATORS>(generators)...) {
            generatorsVector_.resize(sizeof...(GENERATORS));
            tupleToArray(generators_, generatorsVector_, std::make_index_sequence<sizeof...(GENERATORS)>());
        }

        StatefulComposerT(const StatefulComposerT &src) :
                composer_(src.composer_),
                statesCount_(src.statesCount_),
                generators_(src.generators_) {
            generatorsVector_.resize(sizeof...(GENERATORS));
            tupleToArray(generators_, generatorsVector_, std::make_index_sequence<sizeof...(GENERATORS)>());
        }

        bool next() override {
            for (const auto &item : generatorsVector_) {
                if (item->next()) {
                    return true;
                }
            }
            currentState = ((currentState + 1) % statesCount_);
            return (currentState != 0);
        }

        void randomNext() override {
            for (const auto &item : generatorsVector_) {
                item->randomNext();
            }
            currentState = longRand() % statesCount_;
        }

        VALUE getState(DATA &data) const override {
            return std::apply([&](const auto &... generator) {
                return composer_(currentState, generator.getState(data)...);
            }, generators_);
        }

        StateIdType getStateId() const override {
            subStates.clear();
            for (const auto &item : generatorsVector_) {
                subStates.push_back(item->getStateId());
            }
            return StateIdType(currentState, StateIdType(&subStates));
        }


        void setStateId(const std::vector<StateIdType> *stateId) {
            for (int i = 0; i < generatorsVector_.size(); i++) {
                generatorsVector_[i]->setStateId((*stateId)[i]);
            }
        }

        void setStateId(const StateIdType &stateId) override {
            currentState = stateId.asPair().first;
            setStateId(stateId.asPair().second->asVect());
        }

        std::unique_ptr<StatefulComposer> clone() const override {
            return std::unique_ptr<StatefulComposer>(new StatefulComposerT(*this));
        }

    };

    std::unique_ptr<StatefulComposer> statefulComposer_;
public:
    using DataType = DATA;
    using StateType = VALUE;

    inline static size_t maxId = 0;
    size_t id = maxId++;

    template<class... GENERATORS>
    ComposedGenerator(auto &&composer, size_t stateCount, GENERATORS &&... generators) {
        //std::cout << id << std::endl;
        statefulComposer_ = std::unique_ptr<StatefulComposer>(new StatefulComposerT<GENERATORS...>(
                std::forward<decltype(composer)>(composer), stateCount, std::forward<GENERATORS>(generators)...));
    }

    ComposedGenerator(const ComposedGenerator &src) {
        //std::cout << id << std::endl;
        statefulComposer_ = src.statefulComposer_->clone();
    }

    bool next() override {
        return statefulComposer_->next();
    }

    void randomNext() override {
        statefulComposer_->randomNext();
    }

    VALUE getState(DATA &data) const override {
        return statefulComposer_->getState(data);
    }

    StateIdType getStateId() const override {
        return statefulComposer_->getStateId();
    }

    void setStateId(const StateIdType &stateId) override {
        statefulComposer_->setStateId(stateId);
    }
};

template<class VALUE, class DATA>
class RepeatGenerator : public TestDataGenerator<VALUE, DATA> {

    struct StatefulComposer {
        virtual bool next() = 0;

        virtual void randomNext() = 0;

        virtual VALUE getState(DATA &data) const = 0;

        virtual StateIdType getStateId() const = 0;

        virtual void setStateId(const StateIdType &stateId) = 0;

        virtual ~StatefulComposer() {};

        virtual std::unique_ptr<StatefulComposer> clone() const = 0;
    };

    template<class GENERATOR>
    class StatefulComposerT : public StatefulComposer {
        using Composer = std::function<VALUE(size_t, std::vector<typename std::remove_cvref_t<GENERATOR>::StateType>)>;
        Composer composer_;
        std::vector<GENERATOR> generatorsVector_;
        mutable std::vector<StateIdType> subStates = {};
        const size_t statesCount_;
        size_t currentState = 0;
    public:
        StatefulComposerT(Composer &&composer, size_t stateCount, size_t repeatCount, const GENERATOR &generator) :
                composer_(std::forward<Composer>(composer)),
                statesCount_(stateCount) {
            for (int i = 0; i < repeatCount; i++) {
                generatorsVector_.emplace_back(generator);
            }
        }

        StatefulComposerT(const StatefulComposerT &src) :
                composer_(src.composer_),
                statesCount_(src.statesCount_),
                generatorsVector_(src.generatorsVector_) {}

        bool next() override {
            for (auto &item : generatorsVector_) {
                if (item.next()) {
                    return true;
                }
            }
            currentState = ((currentState + 1) % statesCount_);
            return (currentState != 0);
        }

        void randomNext() override {
            for (auto &item : generatorsVector_) {
                item.randomNext();
            }
            currentState = longRand() % statesCount_;
        }

        VALUE getState(DATA &data) const override {
            std::vector<typename std::remove_cvref_t<GENERATOR>::StateType> subData;
            for (auto &generator : generatorsVector_) {
                subData.emplace_back(generator.getState(data));
            }
            return composer_(currentState, subData);
        }

        StateIdType getStateId() const override {
            subStates.clear();
            for (const auto &item : generatorsVector_) {
                subStates.push_back(item.getStateId());
            }
            return StateIdType(currentState, StateIdType(&subStates));
        }


        void setStateId(const std::vector<StateIdType> *stateId) {
            for (int i = 0; i < generatorsVector_.size(); i++) {
                generatorsVector_[i].setStateId((*stateId)[i]);
            }
        }

        void setStateId(const StateIdType &stateId) override {
            currentState = stateId.asPair().first;
            setStateId(stateId.asPair().second->asVect());
        }

        std::unique_ptr<StatefulComposer> clone() const override {
            return std::unique_ptr<StatefulComposer>(new StatefulComposerT(*this));
        }

    };

    std::unique_ptr<StatefulComposer> statefulComposer_;
public:
    using DataType = DATA;
    using StateType = VALUE;

    inline static size_t maxId = 0;
    size_t id = maxId++;

    template<class GENERATOR>
    RepeatGenerator(auto &&composer, size_t repeatCount, GENERATOR &&generator) {
        //std::cout << id << std::endl;
        statefulComposer_ = std::unique_ptr<StatefulComposer>(new StatefulComposerT<GENERATOR>(
                std::forward<decltype(composer)>(composer), 1, repeatCount, std::forward<GENERATOR>(generator)));
    }

    RepeatGenerator(const RepeatGenerator &src) {
        //std::cout << id << std::endl;
        statefulComposer_ = src.statefulComposer_->clone();
    }

    bool next() override {
        return statefulComposer_->next();
    }

    void randomNext() override {
        statefulComposer_->randomNext();
    }

    VALUE getState(DATA &data) const override {
        return statefulComposer_->getState(data);
    }

    StateIdType getStateId() const override {
        return statefulComposer_->getStateId();
    }

    void setStateId(const StateIdType &stateId) override {
        statefulComposer_->setStateId(stateId);
    }
};

template<size_t N, class... GEN>
struct getNth {
    using type = decltype(std::get<N>(std::declval<std::tuple<GEN...>>()));
};

template<size_t N, class... GEN>
using getNth_t = typename getNth<N, GEN...>::type;

template<class StateType, class DataType>
class StackedGenerator : public TestDataGenerator<StateType, DataType> {

    std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> generatorsVector_ = {};
    const size_t statesCount_;
    size_t currentState_ = 0;

    std::function<void(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &,
                       const std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &)> vectorCloner_;
public:

    template<class T, size_t I>
    static void transferOne(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &dest,
                            const std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &src) {
        using Gen = std::remove_cvref_t<T>;
        dest.emplace_back(new Gen(*static_cast<Gen *>(src[I].get())));
    }

    template<class Tuple, size_t ...I>
    static void transfer(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &dest,
                         const std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &src,
                         std::index_sequence<I...>) {
        dest.clear();
        (void) ((transferOne<decltype(std::get<I>(std::declval<Tuple>())), I>(dest, src), 0)+...);
    }

    template<class... GEN>
    explicit StackedGenerator(size_t foo, GEN &&... generators):statesCount_(sizeof...(GEN)) {
        (void) ((generatorsVector_.emplace_back(new std::remove_cvref_t<GEN>(generators)), 0)+...);
        vectorCloner_ = [](std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &dest,
                           const std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &src) {
            transfer<std::tuple<std::remove_cvref_t<GEN>...>>(dest, src, std::make_index_sequence<sizeof...(GEN)>());
        };
    }

    StackedGenerator(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &&generatorsVector,
                     std::function<void(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &,
                                        const std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &)> vectorCloner)
            :
            generatorsVector_(std::move(generatorsVector)), statesCount_(generatorsVector_.size()),
            vectorCloner_(vectorCloner) {
    }

    static StackedGenerator
    buildGenerator(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &&generatorsVector,
                   std::function<void(std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &,
                                      const std::vector<std::unique_ptr<TestDataGenerator<StateType, DataType>>> &)> vectorCloner) {
        return StackedGenerator(std::move(generatorsVector), vectorCloner);
    }


    StackedGenerator(StackedGenerator &&generator) noexcept = default;

    StackedGenerator(StackedGenerator &src) : vectorCloner_(src.vectorCloner_), statesCount_(src.statesCount_) {
        vectorCloner_(generatorsVector_, src.generatorsVector_);
    };

    StackedGenerator(const StackedGenerator &src) : vectorCloner_(src.vectorCloner_), statesCount_(src.statesCount_) {
        vectorCloner_(generatorsVector_, src.generatorsVector_);
    };


    bool next() override {
        if (generatorsVector_[currentState_]->next()) {
            return true;
        }
        currentState_ = ((currentState_ + 1) % statesCount_);
        return (currentState_ != 0);
    }

    void randomNext() override {
        currentState_ = longRand() % statesCount_;
        generatorsVector_[currentState_]->randomNext();
    }

    StateType getState(DataType &data) const override {
        return generatorsVector_[currentState_]->getState(data);
    }

    StateIdType getStateId() const override {
        return StateIdType(currentState_, generatorsVector_[currentState_]->getStateId());
    }

    void setStateId(const StateIdType &stateId) override {
        currentState_ = stateId.asPair().first;
        generatorsVector_[currentState_]->setStateId(*stateId.asPair().second);
    }
};


//Test Data Tool
template<class T, size_t MAX_COUNT, size_t SIZE>
class TDT {
    //Types
public:
    using type = T;
    const size_t MaxCount = MAX_COUNT;
    const size_t Size = SIZE;


    using ANodeT = ANode<T, MAX_COUNT, SIZE>;
    using LeafT = typename ANodeT::LeafT;
    using BNodeT = typename ANodeT::BNodeT;
    using Builder = typename ANodeT::BuilderT;

    using VarTypeT = typename BNodeT::VarType;
    using ANodeVar = typename ANodeT::VarType;


    using LeafPtr = typename ANodeT::LeafPtr;
    using ANodePtr = typename ANodeT::ANodePtr;
    using BNodePtr = typename ANodeT::BNodePtr;
    using LeafCPtr = typename ANodeT::LeafCPtr;
    using ANodeCPtr = typename ANodeT::ANodeCPtr;
    using BNodeCPtr = typename ANodeT::BNodeCPtr;


    LeafT getLeaf(size_t size, T offset, T step, size_t shift = 0) {
        assert(size + shift <= SIZE);
        std::array<T, SIZE> localData;
        LeafT result = LeafT::createLeaf(nullptr);
        T value = offset;
        for (size_t i = 0; i < size; i++, value += step) {
            localData[i + shift] = value;
        }
        result.add(localData.data(), size + shift);
        result.slice(shift, size);
        return result;
    }

    std::vector<T> getVector(size_t size, T offset, T step) {
        assert(size <= SIZE);
        std::vector<T> result(size);
        T value = offset;
        for (size_t i = 0; i < size; i++, value += step) {
            result[i] = value;
        }
        return result;
    }

    enum ShiftStyle {
        Left, Center, Right
    };

    auto getVectorLeaf(size_t size, T offset, T step, ShiftStyle shiftStyle = Left) {
        size_t shift;
        switch (shiftStyle) {
            case Left:
                shift = 0;
            case Center:
                shift = (Size - size) / 2;
            case Right:
                shift = Size - size;
        }
        return std::make_pair(getVector(size, offset, step), getLeaf(size, offset, step, shift));
    }

    LeafT getLeafV(std::initializer_list<T> values) {
        assert(values.size() <= SIZE);
        std::array<T, SIZE> localData;
        LeafT result = LeafT::createLeaf(nullptr);
        std::copy(values.begin(), values.end(), localData.begin());
        result.add(localData.data(), values.size());
        return result;
    }

    static LeafPtr p(LeafT &&LeafT) {
        return LeafT::createLeafPtr(std::move(LeafT));
    }

    static ANodePtr p(ANodeT &&node) {
        return ANodeT::createNodePtr(std::move(node));
    }

    LeafCPtr cp(LeafT &&LeafT, bool isRoot = false) {
        auto tp = p(std::move(LeafT));
        tp->makeConst();
        return closeNode(std::move(tp), isRoot);
    }

    T tValue() {
        return std::declval<T>();
    }

    template<class Node>
    struct Rep {
        size_t count;
        const Node &node;

        Rep(size_t count, const Node &node) : count(count), node(node) {};
    };

    template<class Node>
    struct Sub {
        size_t offset;
        size_t length;
        const Node &node;

        Sub(size_t offset, size_t length, const Node &node) : offset(offset), length(length), node(node) {};
    };

    template<class Node>
    Rep<Node> r(size_t count, const Node &node) { return Rep<Node>(count, node); }

    template<class Parent, class Node>
    void addNode(Parent &parent, Node &&node) {
        if constexpr (is_template_instance<Rep, Node>::value) {
            for (int i = 0; i < node.count; i++) {
                addNode(parent, node.node);
            }
        } else if constexpr (is_template_instance<Sub, Node>::value) {
            parent.addNode(std::forward<Node>(node.node), node.offset, node.length);
        } else {
            parent.addNode(std::forward<Node>(node));
        }
    }

    template<class... NODE>
    BNodeT getBNode(NODE &&...nodes) {
        std::tuple<NODE...> values = {std::forward<NODE>(nodes)...};
        if constexpr (sizeof...(nodes) == 0) {
            return BNodeT(0);
        } else {
            BNodeT result(heightOf(std::get<0>(values)) + 1);
            std::apply([&](auto &&...x) {
                (addNode(result, std::move(x)), ...);
            }, std::move(values));
            return result;
        }
    }

    template<class... NODE>
    ANodeT getANode(NODE &&...nodes) {
        std::tuple<NODE...> values = {std::forward<NODE>(nodes)...};
        if constexpr (sizeof...(nodes) == 0) {
            throw std::logic_error("ANodeT needs at least one child");
        } else {
            ANodeT result(std::get<0>(values));
            std::apply([&](auto &&...x) {
                (addNode(result, std::move(x)), ...);
            }, std::move(values));
            return result;
        }
    }

    BNodePtr p(BNodeT &&BNodeT) {
        return BNodeT::createNodePtr(std::move(BNodeT));
    }

    BNodeCPtr cp(BNodeT &&BNodeT, bool isRoot = false) {
        auto tp = p(std::move(BNodeT));
        tp->makeConst();
        return closeNode(std::move(tp), isRoot);
    }

    ANodeCPtr cp(ANodeT &&ANodeT, bool isRoot = false) {
        auto tp = p(std::move(ANodeT));
        tp->makeConst();
        return closeNode(std::move(tp), isRoot);
    }

    Builder builder() { return Builder(); }

    T typeTest() {
        return T();
    }

    auto getLeafGeneratorImb() {
        return LambdaBasedGeneration<std::pair<std::vector<T>, LeafT>, T>(
                [&](size_t state, T &seed) {
                    ShiftStyle shiftStyle = static_cast<ShiftStyle>(state / 8);
                    state = state % 8;
                    if (state < 2) {
                        auto result = getVectorLeaf(state + 1, seed, 1, shiftStyle);
                        seed += (state + 1);
                        return result;
                    } else if (state < 5) {
                        size_t delta = state - 2;
                        auto result = getVectorLeaf(Size - delta, seed, 1, shiftStyle);
                        seed += (Size - delta);
                        return result;
                    } else if (state < 8) {
                        size_t size = Size / 2 - 1 + (state - 5);
                        auto result = getVectorLeaf(size, seed, 1, shiftStyle);
                        seed += size;
                        return result;
                    }
                    throw std::logic_error("state overflow");
                }, 24);
    }

    auto getBalancedLeafGenerator() {
        return LambdaBasedGeneration<std::pair<std::vector<T>, LeafT>, T>(
                [&](size_t state, T &seed) {
                    ShiftStyle shiftStyle = static_cast<ShiftStyle>(state / 5);
                    state = state % 5;
                    if (state < 3) {
                        size_t delta = state;
                        auto result = getVectorLeaf(Size - delta, seed, 1, shiftStyle);
                        seed += (Size - delta);
                        return result;
                    } else if (state < 5) {
                        size_t size = Size / 2 + (state - 3);
                        auto result = getVectorLeaf(size, seed, 1, shiftStyle);
                        seed += size;
                        return result;
                    }
                    throw std::logic_error("state overflow");
                }, 15);
    }

    auto getLeafGenerator(bool isBalanced) {
        return isBalanced ? getBalancedLeafGenerator() : getLeafGeneratorImb();
    }

    template<class ...GENERATORS>
    auto getComposedGenerator(auto &&composer, size_t stateCount, GENERATORS &&... generators) {
        using StateType = decltype(composer(size_t(0),
                                            std::declval<typename std::remove_cvref_t<GENERATORS>::StateType>()...));
        return ComposedGenerator<StateType, T>(std::forward<decltype(composer)>(composer), stateCount,
                                               std::forward<GENERATORS>(generators)...);
    }

    auto getSlicedOrNotLeafGenerator(bool isBalanced) {
        return getComposedGenerator([&](size_t state, std::pair<std::vector<T>, LeafT> &&dataPair) {
            size_t offset = 0;
            auto &data = dataPair.second;

            size_t length = data.size();
            if (state < 9) {
                if (Size / 2 + (state / 3) + (state % 3) <= length) {
                    offset = std::min(state % 3, length - 1);
                    length -= offset;
                    length = length - std::min(state / 3, length - 1);
                    if (offset || length) {
                        data.slice(offset, length);
                        dataPair.first = std::vector<T>(dataPair.first.begin() + offset,
                                                        dataPair.first.begin() + offset + length);
                    }
                }
            } else {
                throw std::logic_error("state overflow");
            }
            return std::forward<decltype(dataPair)>(dataPair);

        }, 9, getLeafGenerator(isBalanced));
    }

    auto getPointerGenerator(auto &&dataGenerator, bool isRoot = false) {
        return getComposedGenerator([this, isRoot](size_t state, auto &&data) {
            switch (state) {
                case 0:
                    return std::make_pair(std::forward<decltype(data.first)>(data.first),
                                          VarTypeT(p(std::forward<decltype(data.second)>(data.second))));
                case 1:
                    if (!data.second.isDeepBalanced(isRoot)) {
                        return std::make_pair(std::forward<decltype(data.first)>(data.first),
                                              VarTypeT(p(std::forward<decltype(data.second)>(data.second))));
                    }
                    return std::make_pair(std::forward<decltype(data.first)>(data.first),
                                          VarTypeT(cp(std::forward<decltype(data.second)>(data.second), isRoot)));
            };
            throw std::logic_error("State overflow");
        }, 2, std::forward<decltype(dataGenerator)>(dataGenerator));
    }

    auto getNodePointer(size_t height);

    auto getCPointerGenerator(auto &&dataGenerator, bool isRoot = false) {
        return getComposedGenerator([this, isRoot](size_t state, auto &&data) {
            return std::make_pair(std::forward<decltype(data.first)>(data.first),
                                  VarTypeT(cp(std::forward<decltype(data.second)>(data.second), isRoot)));
        }, 1, std::forward<decltype(dataGenerator)>(dataGenerator));
    }

    auto getANodeCPointerGenerator(auto &&dataGenerator, bool isRoot = false) {
        return getComposedGenerator([this, isRoot](size_t state, auto &&data) {
            return std::make_pair(std::forward<decltype(data.first)>(data.first),
                                  ANodeVar(cp(std::forward<decltype(data.second)>(data.second), isRoot)));
        }, 1, std::forward<decltype(dataGenerator)>(dataGenerator));
    }


    template<class...GEN>
    auto getBNodeGeneratorInt(GEN &&...gen) -> ComposedGenerator<std::pair<std::vector<T>, BNodeT>, T> {
        return getComposedGenerator([&](size_t state, auto &&... data) {
            std::vector<T> expected;
            (void) ((expected.insert(expected.end(), data.first.begin(), data.first.end()), 0)+...);
            return std::make_pair(expected, getBNode(std::forward<decltype(data.second)>(data.second)...));
        }, 1, getPointerGenerator(gen)...);
    }

    auto getBNodeGenerator(size_t height, bool isTopBalanced, bool isFrontBalanced, bool isBackBalanced)
    -> StackedGenerator<std::pair<std::vector<T>, BNodeT>, T>;


    template<size_t ...I>
    auto getBNodeGeneratorRep(size_t height, bool isFrontBalanced, bool isBackBalanced, std::index_sequence<I...>) ->
    ComposedGenerator<std::pair<std::vector<T>, BNodeT>, T>;

    template<size_t ChildCount>
    auto getBNodeGenerator(size_t height, bool isFrontBalanced,
                           bool isBackBalanced) -> ComposedGenerator<std::pair<std::vector<T>, BNodeT>, T> {
        if constexpr (ChildCount > 1) {
            return getBNodeGeneratorRep(height, isFrontBalanced, isBackBalanced,
                                        std::make_index_sequence<ChildCount - 2>());
        } else {
            if (height > 1) {
                return getBNodeGeneratorInt(
                        getBNodeGenerator(height - 1, isFrontBalanced, isFrontBalanced, isBackBalanced));
            } else {
                return getBNodeGeneratorInt(getLeafGenerator(isFrontBalanced && isBackBalanced));
            }
        }
    }

    using ANodeResult = std::pair<std::vector<T>, ANodeT>;
    using ANodeVarResult = std::pair<std::vector<T>, ANodeVar>;

    ComposedGenerator<ANodeResult, T> getBNodeBasedANode(int8_t height) {
        return ComposedGenerator<ANodeResult, T>([&](size_t state, std::pair<std::vector<T>, BNodeT> &&bNodeAndState) {
            bool isBalanced = bNodeAndState.second.isBalanced();
            return ANodeResult(std::move(bNodeAndState.first), ANodeT(cp(std::move(bNodeAndState.second), !isBalanced)));
        }, 1, getBNodeGenerator(height, false, true, true));
    };

    ComposedGenerator<ANodeResult, T> getLeafBasedANode() {
        return ComposedGenerator<ANodeResult, T>([&](size_t state, std::pair<std::vector<T>, LeafT> &&bufferAndState) {
            return ANodeResult(std::move(bufferAndState.first), ANodeT(cp(std::move(bufferAndState.second))));
        }, 1, getBalancedLeafGenerator());
    };

    StackedGenerator<ANodeVarResult, T>
    getChildGenerator(int8_t maxHeight) {
        std::vector<std::unique_ptr<TestDataGenerator<ANodeVarResult, T>>> generatorsVector;
        auto bufferGenerator = getANodeCPointerGenerator(getBalancedLeafGenerator(), false);
        using LeafGen = decltype(bufferGenerator);
        generatorsVector.emplace_back(new LambdaBasedGeneration<ANodeVarResult, T>(
                [](size_t state, T &) {
                    return ANodeVarResult({}, {});
                }, 1));
        generatorsVector.emplace_back(new LeafGen(std::move(bufferGenerator)));
        using BNodeGen = std::remove_cvref_t<decltype(getANodeCPointerGenerator(
                getBNodeGenerator(1, true, true, true), false))>;

        for (int8_t i = 1; i <= maxHeight; i++) {
            auto bNodeGenerator = getANodeCPointerGenerator(getBNodeGenerator(i, true, true, true), false);
            generatorsVector.emplace_back(new BNodeGen(std::move(bNodeGenerator)));
        }
        auto vectorCloner = [](std::vector<std::unique_ptr<TestDataGenerator<ANodeVarResult, T>>> &dest,
                               const std::vector<std::unique_ptr<TestDataGenerator<ANodeVarResult, T>>> &src) {
            dest.emplace_back(new LambdaBasedGeneration<ANodeVarResult, T>(
                    [](size_t state, T &) {
                        return ANodeVarResult({}, {});
                    }, 1));
            dest.emplace_back(new LeafGen(*static_cast<LeafGen *>(src[1].get())));
            for (size_t i = 2; i < src.size(); i++) {
                dest.emplace_back(new BNodeGen(*static_cast<BNodeGen *>(src[i].get())));
            }
        };
        return StackedGenerator<ANodeVarResult, T>::buildGenerator(std::move(generatorsVector), vectorCloner);
    }

    RepeatGenerator<std::vector<ANodeVarResult>, T>
    getChildrenGenerator(int8_t maxHeight) {
        return RepeatGenerator<std::vector<ANodeVarResult>, T>(
                [](size_t stateId, std::vector<ANodeVarResult> subResults) {
                    std::vector<ANodeVarResult> result;
                    for (const auto &subResult : subResults) {
                        if (subResult.first.size()) {
                            result.emplace_back(std::move(subResult));
                        }
                    }
                    return result;
                }, MAX_COUNT - 1, getChildGenerator(maxHeight));
    }

    static std::vector<size_t> getPositions(size_t state, size_t distinctCount, size_t totalCount) {
        std::unordered_set<size_t> allDistinct;
        for (size_t i = 0; i < distinctCount; i++) {
            allDistinct.insert(i);
        };
        std::vector<size_t> result;
        size_t runningSeed = state;
        while (allDistinct.size() < (totalCount - result.size())) {
            if (!runningSeed) {
                runningSeed = state * 131;
            }
            size_t usedIndex = runningSeed % distinctCount;
            result.push_back(usedIndex);
            allDistinct.erase(usedIndex);
            runningSeed /= distinctCount;
        }
        std::vector<size_t> leftOvers(allDistinct.begin(), allDistinct.end());
        while (leftOvers.size() > 0) {
            if (!runningSeed) {
                runningSeed = state * 131;
            }
            size_t posToAdd = runningSeed % leftOvers.size();
            result.push_back(leftOvers[posToAdd]);
            leftOvers.erase(leftOvers.begin() + posToAdd);
        }
        assert(result.size() == totalCount);
        return result;
    }

    class Range {
        double offsetRatio_;
        size_t offsetOffset_;
        double lengthRatio_;
        size_t lengthOffset_;
    public:
        Range(double offsetRatio, size_t offsetOffset, double lengthRatio, size_t lengthOffset) :
                offsetRatio_(offsetRatio), offsetOffset_(offsetOffset), lengthRatio_(lengthRatio),
                lengthOffset_(lengthOffset) {};

        std::pair<size_t, size_t> getRange(size_t totalLength, size_t minLength, size_t maxLength) const {
            size_t offset = offsetRatio_ * totalLength + offsetOffset_;
            size_t length = lengthRatio_ * totalLength;
            if (length > lengthOffset_) {
                length -= lengthOffset_;
            } else {
                length = 1;
            }
            length = std::min(maxLength, std::max(minLength, length));
            if (length + offset > totalLength) {
                offset = totalLength - length;
            }
            return {offset, length};
        }
    };

    static auto getANodeWithChildren(bool isRoot) {
        return [isRoot](size_t state, std::pair<std::vector<T>, ANodeT> aNodeEtc,
                        std::vector<std::pair<std::vector<T>, ANodeVar>> childValues) -> std::pair<std::vector<T>, ANodeT> {
            static const std::array<Range, 8> ranges{
                    {
                            {0.0, 0, 1.0, 0},
                            {0, 1, 1, 0},
                            {0, 0, 1, 1},
                            {0, 1, 1, 1},
                            {.25, 0, .75, 0},
                            {.25, 1, .75, 0},
                            {.25, 0, .75, 1},
                            {.25, 1, .75, 1},
                    }};
            size_t dist = childValues.size() + 1;
            std::vector<size_t> positions = getPositions(state, dist, dist +
                                                                      (MAX_COUNT > dist ? state % (MAX_COUNT - dist)
                                                                                        : 0));
            size_t lastZeroPos = 0;
            for (size_t i = 0; i < positions.size(); i++) {
                if (!positions[i]) {
                    lastZeroPos = i;
                }
            }
            ANodeT &resultNode = aNodeEtc.second;
            std::vector<T> resultData;
            static ANodeVar nullNode;
            size_t minAddToOrigin = isRoot ? 1 : resultNode.minRetention();
            int pos = 0;
            size_t runningSeed = state;
            bool isBuffNode = aNodeEtc.second.height() == 0;

            for (const auto &item : positions) {
                ANodeVar *nodeToAdd;
                size_t originalLength;
                size_t minRetainedSize;
                if (item) {
                    ANodeVar &incomingNode = childValues[item - 1].second;
                    originalLength = sizeOf(incomingNode);
                    if (incomingNode.index() == 1) {
                        minRetainedSize = resultNode.minChildRetention(std::get<BNodeCPtr>(incomingNode));
                    } else {
                        minRetainedSize = 1;
                    }
                    nodeToAdd = &incomingNode;
                } else {
                    nodeToAdd = &nullNode;
                    originalLength = resultNode.originSize();
                    if (pos == lastZeroPos) {
                        minRetainedSize = minAddToOrigin;
                    } else {
                        minRetainedSize = 1;
                    }
                }
                if (!runningSeed) {
                    runningSeed = state * 131;
                }
                size_t usedIndex = runningSeed % ranges.size();
                runningSeed /= ranges.size();
                auto range = ranges[usedIndex].getRange(originalLength, minRetainedSize,
                                                        isBuffNode ? SIZE * 2 / MAX_COUNT : originalLength);
                resultNode.addNodeVar(*nodeToAdd, range.first, range.second);
                if (item) {
                    auto &childData = childValues[item - 1].first;
                    resultData.insert(resultData.end(), childData.begin() + range.first,
                                      childData.begin() + range.first + range.second);
                } else {
                    if (range.second < minAddToOrigin) {
                        minAddToOrigin -= range.second;
                    } else {
                        minAddToOrigin = 1;
                    }
                    auto &childData = aNodeEtc.first;
                    resultData.insert(resultData.end(), childData.begin() + range.first,
                                      childData.begin() + range.first + range.second);
                }
                pos++;
            }
            return {resultData, std::move(aNodeEtc.second)};
        };
    }

    ComposedGenerator<std::pair<std::vector<T>, ANodeT>, T> getANodeGenerator(int8_t height, bool isRoot = true) {


        return ComposedGenerator<std::pair<std::vector<T>, ANodeT>, T>(getANodeWithChildren(isRoot),
                                                                      std::numeric_limits<size_t>::max(),
                                                                      (height ? getBNodeBasedANode(height)
                                                                              : getLeafBasedANode()),
                                                                      getChildrenGenerator(
                                                                              height ? height - 1 : 0));
    }
};

template<class ... GEN>
auto getStackedGenerator(GEN &&... generators) {
    using GenType = std::remove_cvref_t<getNth_t<0, GEN...>>;
    using StateType = typename GenType::StateType;
    using DataType = typename GenType::DataType;
    return StackedGenerator<StateType, DataType>(0, std::forward<GEN>(generators)...);
}

template<class T, size_t MAX_COUNT, size_t SIZE>
auto TDT<T, MAX_COUNT, SIZE>::getBNodeGenerator(size_t height, bool isTopBalanced, bool isFrontBalanced,
                                                bool isBackBalanced) -> StackedGenerator<std::pair<std::vector<T>, BNodeT>, T> {
    if (isTopBalanced) {
        return getStackedGenerator(
                getBNodeGenerator<MAX_COUNT / 2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT / 2 + 1>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT / 2 + 2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT - 2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT - 1>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT>(height, isFrontBalanced, isBackBalanced));
    } else {
        return getStackedGenerator(
                getBNodeGenerator<1>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT / 2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT / 2 + 1>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT / 2 + 2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT - 2>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT - 1>(height, isFrontBalanced, isBackBalanced),
                getBNodeGenerator<MAX_COUNT>(height, isFrontBalanced, isBackBalanced));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE>
template<size_t ...I>
auto TDT<T, MAX_COUNT, SIZE>::getBNodeGeneratorRep(size_t height, bool isFrontBalanced, bool isBackBalanced,
                                                   std::index_sequence<I...>) -> ComposedGenerator<std::pair<std::vector<T>, BNodeT>, T> {
    if (height > 1) {
        return getBNodeGeneratorInt(getBNodeGenerator(height - 1, isFrontBalanced, isFrontBalanced, true),
                                    ((void) I, getBNodeGenerator(height - 1, true, true, true))...,
                                    getBNodeGenerator(height - 1, isBackBalanced, true, isBackBalanced));
    } else {
        return getBNodeGeneratorInt(getLeafGenerator(isFrontBalanced),
                                    ((void) I, getLeafGenerator(true))...,
                                    getLeafGenerator(isBackBalanced));
    }
}

template<class T, size_t MAX_COUNT, size_t SIZE>
auto TDT<T, MAX_COUNT, SIZE>::getNodePointer(size_t height) {
    if (height) {
        return getStackedGenerator(
                getPointerGenerator(getBNodeGenerator(height, false, false, false), true),
                getPointerGenerator(getANodeGenerator(height), true));
    } else {
        return getStackedGenerator(
                getPointerGenerator(getSlicedOrNotLeafGenerator(false), true),
                getPointerGenerator(getANodeGenerator(0, true), true));
    }
}

#endif //EXPERIMENTS_TESTDATATOOL_H
