#ifndef EXPERIMENTS_BTRAITS_H
#define EXPERIMENTS_BTRAITS_H

template<template<typename...> class, typename>
struct is_template_instance : std::false_type {
};

template<template<typename...> class T, class... ARGS>
struct is_template_instance<T, T<ARGS...>> : std::true_type {
};

template<class T>
constexpr bool is_unique_ptr_v = is_template_instance<std::unique_ptr, std::remove_cvref_t<T>>::value;

template<class T>
constexpr bool is_shared_ptr_v = is_template_instance<std::shared_ptr, std::remove_cvref_t<T>>::value;


#endif //EXPERIMENTS_BTRAITS_H
