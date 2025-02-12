#pragma once

#include <format>
#include <string>

#include "common.h"

struct TableEntity {
    std::string table;
    int         id;

    auto operator<=>(const TableEntity&) const = default;
};

namespace std {
template <>
struct hash<TableEntity> {
    size_t operator()(const TableEntity& te) const noexcept {
        size_t seed = 0;
        hash_combine(seed, hash<string>{}(te.table));
        hash_combine(seed, hash<int>{}(te.id));
        return seed;
    }
};

template <>
struct formatter<TableEntity> {
    template <class ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FormatContext>
    auto format(const TableEntity& te, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "({}, {})", te.table, te.id);
    }
};

} // namespace std
