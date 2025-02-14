#include <array>
#include <fmt/core.h>
#include <memory>
#include <unordered_map>

#include <plan.h>
#include <table.h>
#include <table_entity.h>

#include <nlohmann/json.hpp>

#include <SQLParser.h>

using json = nlohmann::json;

// 属性查找表
std::unordered_map<std::string, std::vector<Attribute>> attributes_map = {
    {"aka_name",
     {{DataType::INT32, "id"},
            {DataType::INT32, "person_id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::VARCHAR, "name_pcode_cf"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "surname_pcode"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"aka_title",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::VARCHAR, "title"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "kind_id"},
            {DataType::INT32, "production_year"},
            {DataType::VARCHAR, "phonetic_code"},
            {DataType::INT32, "episode_of_id"},
            {DataType::INT32, "season_nr"},
            {DataType::INT32, "episode_nr"},
            {DataType::VARCHAR, "note"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"cast_info",
     {{DataType::INT32, "id"},
            {DataType::INT32, "person_id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "person_role_id"},
            {DataType::VARCHAR, "note"},
            {DataType::INT32, "nr_order"},
            {DataType::INT32, "role_id"}}                                     },
    {"char_name",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "surname_pcode"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"comp_cast_type",  {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"company_name",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "country_code"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "name_pcode_sf"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"company_type",    {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"complete_cast",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "subject_id"},
            {DataType::INT32, "status_id"}}                                   },
    {"info_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "info"}}},
    {"keyword",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "keyword"},
            {DataType::VARCHAR, "phonetic_code"}}                             },
    {"kind_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"link_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "link"}}},
    {"movie_companies",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "company_id"},
            {DataType::INT32, "company_type_id"},
            {DataType::VARCHAR, "note"}}                                      },
    {"movie_info_idx",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "info_type_id"},
            {DataType::VARCHAR, "info"},
            {DataType::VARCHAR, "note"}}                                      },
    {"movie_keyword",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "keyword_id"}}                                  },
    {"movie_link",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "linked_movie_id"},
            {DataType::INT32, "link_type_id"}}                                },
    {"name",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "gender"},
            {DataType::VARCHAR, "name_pcode_cf"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "surname_pcode"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"role_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "role"}}},
    {"title",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "title"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "kind_id"},
            {DataType::INT32, "production_year"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "phonetic_code"},
            {DataType::INT32, "episode_of_id"},
            {DataType::INT32, "season_nr"},
            {DataType::INT32, "episode_nr"},
            {DataType::VARCHAR, "series_years"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"movie_info",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "info_type_id"},
            {DataType::VARCHAR, "info"},
            {DataType::VARCHAR, "note"}}                                      },
    {"person_info",
     {{DataType::INT32, "id"},
            {DataType::INT32, "person_id"},
            {DataType::INT32, "info_type_id"},
            {DataType::VARCHAR, "info"},
            {DataType::VARCHAR, "note"}}                                      }
};

using OutputAttrsType = std::vector<std::tuple<TableEntity, std::string>>;
using AliasMapType    = std::unordered_map<std::string, TableEntity>;
using FilterMapType   = std::unordered_map<TableEntity, std::unique_ptr<Statement>>;
using JoinGraphType   = std::unordered_map<TableEntity,
      std::unordered_map<TableEntity, std::tuple<std::string, std::string>>>;
using ColumnMapType = std::unordered_map<TableEntity, std::unordered_map<std::string, size_t>>;

bool only_contains_hash_join(const json& node) {
    std::string_view node_type = node["Node Type"].get<std::string_view>();
    if (node_type == "Nested Loop" or node_type == "Merge Join") {
        return false;
    }
    if (node.contains("nodes")) {
        for (auto& child: node["Plans"]) {
            if (not only_contains_hash_join(child)) {
                return false;
            }
        }
    }
    return true;
}

Plan load_join_pipeline(const json& node,
    const AliasMapType&             alias_map,
    const FilterMapType&            filters,
    const JoinGraphType&            join_graph,
    const OutputAttrsType&          top_attrs) {
    namespace fs = std::filesystem;
    static std::unordered_set<std::string_view> other_operators{"Aggregate", "Gather"};
    static std::unordered_set<std::string_view> join_types{"Nested Loop",
        "Hash Join",
        "Merge Join"};
    static std::unordered_set<std::string_view> scan_types{"Seq Scan", "Index Only Scan"};
    Plan                                        ret;

    auto extract_entities = [&](auto&& extract_entities,
                                const json& node) -> std::unordered_set<TableEntity> {
        auto node_type = node["Node Type"].get<std::string_view>();

        if (auto itr = other_operators.find(node_type); itr != other_operators.end()) {
            return extract_entities(extract_entities, node["Plans"][0]);
        } else if (auto itr = join_types.find(node_type); itr != join_types.end()) {
            if (node_type != "Hash Join") {
                throw std::runtime_error("Not Hash Join");
            }
            auto left_type  = node["Plans"][0]["Node Type"].get<std::string_view>();
            auto right_type = node["Plans"][1]["Node Type"].get<std::string_view>();
            std::unordered_set<TableEntity> left_entities, right_entities;
            if (left_type == "Hash" and right_type != "Hash") {
                left_entities =
                    extract_entities(extract_entities, node["Plans"][0]["Plans"][0]);
                right_entities = extract_entities(extract_entities, node["Plans"][1]);
            } else if (left_type != "Hash" and right_type == "Hash") {
                left_entities = extract_entities(extract_entities, node["Plans"][0]);
                right_entities =
                    extract_entities(extract_entities, node["Plans"][1]["Plans"][0]);
            } else {
                throw std::runtime_error("Hash Join should have at least one Hash child");
            }
            left_entities.merge(std::move(right_entities));
            return left_entities;
        } else if (auto itr = scan_types.find(node_type); itr != scan_types.end()) {
            if (not node.contains("Alias")) {
                throw std::runtime_error("No \"Alias\" in scan node");
            }
            auto        alias = node["Alias"].get<std::string>();
            TableEntity entity;
            if (auto itr = alias_map.find(alias); itr != alias_map.end()) {
                entity = itr->second;
            } else {
                throw std::runtime_error(fmt::format("Cannot find alias: {}", alias));
            }
            std::unordered_set<TableEntity> entities{std::move(entity)};
            return entities;
        } else {
            throw std::runtime_error(fmt::format("Not supported node type: {}", node_type));
        }
    };

    auto recurse = [&](auto&& recurse, const json& node, const OutputAttrsType& required_attrs)
        -> std::tuple<size_t, std::vector<std::tuple<TableEntity, std::string, DataType>>> {
        auto node_type = node["Node Type"].get<std::string_view>();

        if (auto itr = other_operators.find(node_type); itr != other_operators.end()) {
            return recurse(recurse, node["Plans"][0], required_attrs);
        } else if (auto itr = join_types.find(node_type); itr != join_types.end()) {
            if (node_type != "Hash Join") {
                throw std::runtime_error("Not Hash Join");
            }
            auto   left_type  = node["Plans"][0]["Node Type"].get<std::string_view>();
            auto   right_type = node["Plans"][1]["Node Type"].get<std::string_view>();
            bool   build_left;
            size_t left, right;
            size_t left_attr, right_attr;
            std::unordered_set<TableEntity> left_entities, right_entities;
            std::vector<std::tuple<TableEntity, std::string, DataType>> left_columns,
                right_columns;
            TableEntity left_entity, right_entity;
            std::string left_column, right_column;
            const json* pleft;
            const json* pright;
            if (left_type == "Hash" and right_type != "Hash") {
                build_left = true;
                pleft      = &node["Plans"][0]["Plans"][0];
                pright     = &node["Plans"][1];
            } else if (left_type != "Hash" and right_type == "Hash") {
                build_left = false;
                pleft      = &node["Plans"][0];
                pright     = &node["Plans"][1]["Plans"][0];
            } else {
                throw std::runtime_error("Hash Join should have at least one Hash child");
            }
            left_entities             = extract_entities(extract_entities, *pleft);
            right_entities            = extract_entities(extract_entities, *pright);
            bool found_join_condition = false;
            for (auto& entity: left_entities) {
                if (auto itr = join_graph.find(entity); itr != join_graph.end()) {
                    for (auto& [adj, columns]: itr->second) {
                        if (auto iter = right_entities.find(adj);
                            iter != right_entities.end()) {
                            left_entity                         = entity;
                            right_entity                        = adj;
                            std::tie(left_column, right_column) = columns;
                            found_join_condition                = true;
                        }
                    }
                }
            }
            if (not found_join_condition) {
                fmt::println(stderr, "left entities:");
                for (auto& entity: left_entities) {
                    fmt::println(stderr, "    {}", entity);
                }
                fmt::println(stderr, "right entities:");
                for (auto& entity: right_entities) {
                    fmt::println(stderr, "    {}", entity);
                }
                throw std::runtime_error("Cannot find join condition");
            }
            OutputAttrsType left_required, right_required;
            bool            left_attr_already_in = false, right_attr_already_in = false;
            for (const auto& [required_entity, required_column]: required_attrs) {
                if (auto itr = left_entities.find(required_entity);
                    itr != left_entities.end()) {
                    if (required_entity == left_entity and required_column == left_column) {
                        left_attr_already_in = true;
                    }
                    left_required.emplace_back(required_entity, required_column);
                } else if (auto itr = right_entities.find(required_entity);
                    itr != right_entities.end()) {
                    if (required_entity == right_entity and required_column == right_column) {
                        right_attr_already_in = true;
                    }
                    right_required.emplace_back(required_entity, required_column);
                } else {
                    throw std::runtime_error(
                        "Required attributes cannot be found in neither left child nor right "
                        "child.");
                }
            }
            if (not left_attr_already_in) {
                left_required.emplace_back(left_entity, left_column);
            }
            if (not right_attr_already_in) {
                right_required.emplace_back(right_entity, right_column);
            }
            std::tie(left, left_columns)   = recurse(recurse, *pleft, left_required);
            std::tie(right, right_columns) = recurse(recurse, *pright, right_required);
            size_t idx                     = 0;
            bool   left_attr_set = false, right_attr_set = false;
            for (auto& [entity, column, _]: left_columns) {
                if (entity == left_entity and column == left_column) {
                    left_attr     = idx;
                    left_attr_set = true;
                    break;
                }
                ++idx;
            }
            idx = 0;
            for (auto& [entity, column, _]: right_columns) {
                if (entity == right_entity and column == right_column) {
                    right_attr     = idx;
                    right_attr_set = true;
                    break;
                }
                ++idx;
            }
            if (not left_attr_set or not right_attr_set) {
                throw std::runtime_error("Join conditions are not set properly");
            }
            left_columns.insert(left_columns.end(),
                std::make_move_iterator(right_columns.begin()),
                std::make_move_iterator(right_columns.end()));
            std::vector<std::tuple<TableEntity, std::string, DataType>> output_columns;
            std::vector<std::tuple<size_t, DataType>>                   output_attrs;
            for (const auto& [required_entity, required_column]: required_attrs) {
                bool   found     = false;
                size_t input_idx = 0;
                for (const auto& [entity, column, type]: left_columns) {
                    if (entity == required_entity and column == required_column) {
                        output_columns.emplace_back(required_entity, required_column, type);
                        output_attrs.emplace_back(input_idx, type);
                        found = true;
                        break;
                    }
                    ++input_idx;
                }
                if (not found) {
                    throw std::runtime_error(fmt::format(
                        "Cannot found the required attr: {}.{} in children's output",
                        required_entity,
                        required_column));
                }
            }
            auto new_node_id = ret.new_join_node(build_left,
                left,
                right,
                left_attr,
                right_attr,
                std::move(output_attrs));
            return {new_node_id, std::move(output_columns)};
        } else if (auto itr = scan_types.find(node_type); itr != scan_types.end()) {
            if (not node.contains("Alias")) {
                throw std::runtime_error("No \"Alias\" in scan node");
            }
            auto        alias = node["Alias"].get<std::string>();
            TableEntity entity;
            if (auto itr = alias_map.find(alias); itr != alias_map.end()) {
                entity = itr->second;
            } else {
                throw std::runtime_error(fmt::format("Cannot find alias: {}", alias));
            }
            std::vector<Attribute>* pattributes;
            if (auto itr = attributes_map.find(entity.table); itr != attributes_map.end()) {
                pattributes = &itr->second;
            } else {
                throw std::runtime_error(
                    fmt::format("Cannot find attributes for table: {}", entity.table));
            }
            Statement* filter = nullptr;
            if (auto itr = filters.find(entity); itr != filters.end()) {
                filter = itr->second.get();
            }
            auto table        = Table::from_csv(*pattributes,
                fs::path("imdb") / fmt::format("{}.csv", entity.table),
                filter);
            auto new_input_id = ret.new_input(table.to_columnar());
            // auto new_input_id = ret.new_table(std::move(table));
            std::vector<std::tuple<TableEntity, std::string, DataType>> output_columns;
            std::vector<std::tuple<size_t, DataType>>                   output_attrs;
            for (const auto& [required_entity, required_column]: required_attrs) {
                bool   found     = false;
                size_t input_idx = 0;
                for (const auto& attribute: *pattributes) {
                    if (entity == required_entity and attribute.name == required_column) {
                        output_columns.emplace_back(required_entity,
                            required_column,
                            attribute.type);
                        output_attrs.emplace_back(input_idx, attribute.type);
                        found = true;
                        break;
                    }
                    ++input_idx;
                }
                if (not found) {
                    throw std::runtime_error(fmt::format(
                        "Cannot found the required attr: {}.{} in children's output",
                        required_entity,
                        required_column));
                }
            }
            auto new_node_id = ret.new_scan_node(new_input_id, std::move(output_attrs));
            return {new_node_id, std::move(output_columns)};
        } else {
            throw std::runtime_error(fmt::format("Not supported node type: {}", node_type));
        }
    };

    std::tie(ret.root, std::ignore) = recurse(recurse, node, top_attrs);
    return ret;
}

template <>
struct fmt::formatter<hsql::ExprType> {
    template <class ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FormatContext>
    auto format(hsql::ExprType value, FormatContext& ctx) const {
        static std::array<std::string_view, 17> names{
            "kExprLiteralFloat",
            "kExprLiteralString",
            "kExprLiteralInt",
            "kExprLiteralNull",
            "kExprLiteralDate",
            "kExprLiteralInterval",
            "kExprStar",
            "kExprParameter",
            "kExprColumnRef",
            "kExprFunctionRef",
            "kExprOperator",
            "kExprSelect",
            "kExprHint",
            "kExprArray",
            "kExprArrayIndex",
            "kExprExtract",
            "kExprCast",
        };
        return fmt::format_to(ctx.out(), "{}", names[int(value)]);
    }
};

template <>
struct fmt::formatter<hsql::OperatorType> {
    template <class ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FormatContext>
    auto format(hsql::OperatorType value, FormatContext& ctx) const {
        static std::array<std::string_view, 27> names{
            "kOpNone",

            // Ternary operator
            "kOpBetween",

            // n-nary special case
            "kOpCase",
            "kOpCaseListElement", // `WHEN expr THEN expr`

            // Binary operators.
            "kOpPlus",
            "kOpMinus",
            "kOpAsterisk",
            "kOpSlash",
            "kOpPercentage",
            "kOpCaret",

            "kOpEquals",
            "kOpNotEquals",
            "kOpLess",
            "kOpLessEq",
            "kOpGreater",
            "kOpGreaterEq",
            "kOpLike",
            "kOpNotLike",
            "kOpILike",
            "kOpAnd",
            "kOpOr",
            "kOpIn",
            "kOpConcat",

            // Unary operators.
            "kOpNot",
            "kOpUnaryMinus",
            "kOpIsNull",
            "kOpExists",
        };
        return fmt::format_to(ctx.out(), "{}", names[int(value)]);
    }
};

std::tuple<std::string, TableEntity> extract_column_and_table(hsql::Expr* expr,
    const std::unordered_map<std::string, int>&                           table_counts,
    const std::unordered_map<std::string, std::vector<std::string>>&      column_to_tables,
    const AliasMapType&                                                   alias_map) {
    namespace views = ranges::views;
    using namespace std::string_literals;
    std::string column = expr->name;
    TableEntity table_entity;
    if (expr->hasTable()) {
        auto table = expr->table;
        if (auto itr = alias_map.find(table); itr != alias_map.end()) {
            table_entity = itr->second;
        } else if (auto itr = table_counts.find(table); itr != table_counts.end()) {
            auto count = itr->second;
            if (count == 1) {
                table_entity = {table, 0};
            } else {
                throw std::runtime_error(fmt::format("Ambiguous table: {}", table));
            }
        } else {
            throw std::runtime_error(fmt::format("Unknown table name: {}", table));
        }
    } else {
        if (auto itr = column_to_tables.find(column); itr != column_to_tables.end()) {
            if (itr->second.size() > 1) {
                throw std::runtime_error(
                    fmt::format("Ambiguous column: {0}, {1} have column {0}",
                        column,
                        itr->second | views::join(", ") | ranges::to<std::string>()));
            } else {
                auto& table_name = itr->second[0];
                if (auto itr = table_counts.find(table_name); itr != table_counts.end()) {
                    auto count = itr->second;
                    if (count == 1) {
                        table_entity = {table_name, 0};
                    } else {
                        throw std::runtime_error(
                            fmt::format("Ambiguous table: {}", table_name));
                    }
                }
            }
        } else {
            throw std::runtime_error(fmt::format("No such column: {}", column));
        }
    }
    return {column, table_entity};
}

void assert_column(hsql::Expr* expr) {
    if (expr->type != hsql::kExprColumnRef) {
        throw std::runtime_error(
            fmt::format("left side of \"Equals\" condition must be a ColumnRef"));
        exit(1);
    }
}

void
insert_filter(FilterMapType& filters, TableEntity entity, std::unique_ptr<Statement> stmt) {
    if (auto itr = filters.find(entity); itr == filters.end()) {
        filters.emplace(std::move(entity), std::move(stmt));
    } else {
        auto new_stmt = LogicalOperation::makeAnd(std::move(itr->second), std::move(stmt));
        itr->second   = std::move(new_stmt);
    }
}

// if there is no And, this will cause error
void parse_expr(hsql::Expr*                                          expr,
    const std::unordered_map<std::string, int>&                      table_counts,
    const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables,
    const AliasMapType&                                              alias_map,
    const ColumnMapType&                                             column_map,
    FilterMapType&                                                   filters,
    DSU&                                                             join_union,
    std::unique_ptr<Statement>*                                      out_statement = nullptr,
    TableEntity* out_entity                                                        = nullptr,
    int level                                                                      = 0) {
    namespace views = ranges::views;
    switch (expr->type) {
    case hsql::kExprOperator: {
        auto op_type = expr->opType;
        switch (op_type) {
        case hsql::kOpAnd:
        case hsql::kOpOr:  {
            int add = 0;
            if (op_type == hsql::kOpAnd) {
                // fmt::println("operator And");
            } else {
                // fmt::println("operator Or");
                add = 1;
            }
            std::unique_ptr<Statement> left;
            TableEntity                left_entity;
            std::unique_ptr<Statement> right;
            TableEntity                right_entity;
            // fmt::println("parse left");
            parse_expr(expr->expr,
                table_counts,
                column_to_tables,
                alias_map,
                column_map,
                filters,
                join_union,
                &left,
                &left_entity,
                level + add);
            // fmt::println("parse right");
            parse_expr(expr->expr2,
                table_counts,
                column_to_tables,
                alias_map,
                column_map,
                filters,
                join_union,
                &right,
                &right_entity,
                level + add);
            if (level == 0 and op_type == hsql::kOpAnd) {
                // fmt::println("Top level And");
                if (left) {
                    // fmt::println("insert left");
                    left->pretty_print();
                    insert_filter(filters, std::move(left_entity), std::move(left));
                }
                if (right) {
                    // fmt::println("insert right: {}", (void*)right.get());
                    right->pretty_print();
                    insert_filter(filters, std::move(right_entity), std::move(right));
                }
            } else {
                if (!left or !right) {
                    throw std::runtime_error(
                        "Non top level contains join condition instead of filter");
                }
                if (left_entity != right_entity) {
                    throw std::runtime_error("Filter can not be pushed down");
                }
                if (op_type == hsql::kOpAnd) {
                    *out_statement =
                        LogicalOperation::makeAnd(std::move(left), std::move(right));
                } else {
                    *out_statement =
                        LogicalOperation::makeOr(std::move(left), std::move(right));
                }
                *out_entity = left_entity;
            }
            break;
        }
        case hsql::kOpNot: {
            // fmt::println("operator Not");
            // fmt::println("parse child");
            std::unique_ptr<Statement> child;
            TableEntity                child_entity;
            parse_expr(expr->expr,
                table_counts,
                column_to_tables,
                alias_map,
                column_map,
                filters,
                join_union,
                &child,
                &child_entity,
                level + 1);
            *out_statement = LogicalOperation::makeNot(std::move(child));
            *out_entity    = std::move(child_entity);
            break;
        }
        case hsql::kOpLess:
        case hsql::kOpLessEq:
        case hsql::kOpGreater:
        case hsql::kOpGreaterEq:
        case hsql::kOpEquals:
        case hsql::kOpNotEquals: {
            Comparison::Op op;
            // switch (op_type) {
            // case hsql::kOpLess:      fmt::println("operator Less"); break;
            // case hsql::kOpLessEq:    fmt::println("operator LessEq"); break;
            // case hsql::kOpGreater:   fmt::println("operator Greater"); break;
            // case hsql::kOpGreaterEq: fmt::println("operator GreaterEq"); break;
            // case hsql::kOpEquals:    fmt::println("operator Equals"); break;
            // case hsql::kOpNotEquals: fmt::println("operator Not Equals"); break;
            // default:                 std::unreachable();
            // }
            switch (op_type) {
            case hsql::kOpLess:      op = Comparison::Op::LT; break;
            case hsql::kOpLessEq:    op = Comparison::Op::LEQ; break;
            case hsql::kOpGreater:   op = Comparison::Op::GT; break;
            case hsql::kOpGreaterEq: op = Comparison::Op::GEQ; break;
            case hsql::kOpEquals:    op = Comparison::Op::EQ; break;
            case hsql::kOpNotEquals: op = Comparison::Op::NEQ; break;
            default:                 unreachable();
            }
            auto left  = expr->expr;
            auto right = expr->expr2;
            assert_column(left);
            auto [left_column, left_entity] =
                extract_column_and_table(left, table_counts, column_to_tables, alias_map);
            // fmt::println("left_column: {}", left_column);
            // fmt::println("left_table: {}", left_table);
            Literal value;
            switch (right->type) {
            case hsql::kExprLiteralInt: {
                // fmt::println("int literal: {}", right->ival);
                value = right->ival;
                break;
            }
            case hsql::kExprLiteralString: {
                // fmt::println("string literal: {}", right->name);
                value = right->name;
                break;
            }
            case hsql::kExprColumnRef: {
                auto [right_column, right_entity] =
                    extract_column_and_table(right, table_counts, column_to_tables, alias_map);
                if (op_type != hsql::kOpEquals) {
                    throw std::runtime_error("Non-EuqalJoins are not supported");
                }
                size_t left_column_idx, right_column_idx;
                if (auto itr = column_map.find(left_entity); itr != column_map.end()) {
                    if (auto iter = itr->second.find(left_column); iter != itr->second.end()) {
                        left_column_idx = iter->second;
                    } else {
                        throw std::runtime_error(fmt::format("No column: {} in table: {}",
                            left_column,
                            left_entity));
                    }
                } else {
                    throw std::runtime_error(fmt::format("No  table: {}", left_entity));
                }
                if (auto itr = column_map.find(right_entity); itr != column_map.end()) {
                    if (auto iter = itr->second.find(right_column); iter != itr->second.end()) {
                        right_column_idx = iter->second;
                    } else {
                        throw std::runtime_error(fmt::format("No column: {} in table: {}",
                            right_column,
                            right_entity));
                    }
                } else {
                    throw std::runtime_error(fmt::format("No  table: {}", right_entity));
                }
                join_union.unite(left_column_idx, right_column_idx);
                break;
            }
            default:
                throw std::runtime_error(
                    fmt::format("Expression type: {} not processed", right->type));
            }
            if (right->type != hsql::kExprColumnRef) {
                *out_statement =
                    std::make_unique<Comparison>(std::move(left_column), op, std::move(value));
                *out_entity = std::move(left_entity);
            }
            break;
        }
        case hsql::kOpLike:
        case hsql::kOpNotLike: {
            Comparison::Op op;
            if (op_type == hsql::kOpLike) {
                op = Comparison::Op::LIKE;
                // fmt::println("operator Like");
            } else {
                op = Comparison::Op::NOT_LIKE;
                // fmt::println("operator Not Like");
            }
            auto left  = expr->expr;
            auto right = expr->expr2;
            assert_column(left);
            auto [left_column, left_entity] =
                extract_column_and_table(left, table_counts, column_to_tables, alias_map);
            // fmt::println("left_column: {}", left_column);
            // fmt::println("left_table: {}", left_table);
            switch (right->type) {
            case hsql::kExprLiteralString: {
                // fmt::println("string literal: {}", right->name);
                Literal value = right->name;
                *out_statement =
                    std::make_unique<Comparison>(std::move(left_column), op, std::move(value));
                *out_entity = std::move(left_entity);
                break;
            }
            default:
                throw std::runtime_error(
                    fmt::format("Expression type: {} not processed", right->type));
            }
            break;
        }
        case hsql::kOpBetween: {
            // fmt::println("operator Between");
            auto  left = expr->expr;
            auto& list = *expr->exprList;
            assert_column(left);
            auto [left_column, left_entity] =
                extract_column_and_table(left, table_counts, column_to_tables, alias_map);
            // fmt::println("left_column: {}", left_column);
            // fmt::println("left_table: {}", left_table);
            Literal values[2];
            for (auto [idx, item]: list | views::enumerate) {
                switch (item->type) {
                case hsql::kExprLiteralInt: {
                    // fmt::println("item {}: int literal: {}", idx, item->ival);
                    values[idx] = item->ival;
                    break;
                }
                case hsql::kExprLiteralString: {
                    // fmt::println("item {}: string literal: {}", idx, item->name);
                    values[idx] = item->name;
                    break;
                }
                default:
                    throw std::runtime_error(
                        fmt::format("Expression type: {} not processed", item->type));
                }
            }
            auto stmt1     = std::make_unique<Comparison>(left_column,
                Comparison::Op::GEQ,
                std::move(values[0]));
            auto stmt2     = std::make_unique<Comparison>(left_column,
                Comparison::Op::LEQ,
                std::move(values[1]));
            *out_statement = LogicalOperation::makeAnd(std::move(stmt1), std::move(stmt2));
            *out_entity    = std::move(left_entity);
            break;
        }
        case hsql::kOpIn: {
            // fmt::println("operator In");
            auto  left = expr->expr;
            auto& list = *expr->exprList;
            assert_column(left);
            auto [left_column, left_entity] =
                extract_column_and_table(left, table_counts, column_to_tables, alias_map);
            // fmt::println("left_column: {}", left_column);
            // fmt::println("left_table: {}", left_table);
            for (auto [idx, item]: list | views::enumerate) {
                Literal value;
                switch (item->type) {
                case hsql::kExprLiteralInt: {
                    // fmt::println("item {}: int literal: {}", idx, item->ival);
                    value = item->ival;
                    break;
                }
                case hsql::kExprLiteralString: {
                    // fmt::println("item {}: string literal: {}", idx, item->name);
                    value = item->name;
                    break;
                }
                default:
                    throw std::runtime_error(
                        fmt::format("Expression type: {} not processed", item->type));
                }
                if (not*out_statement) {
                    *out_statement = std::make_unique<Comparison>(left_column,
                        Comparison::Op::EQ,
                        std::move(value));
                } else {
                    auto new_stmt  = std::make_unique<Comparison>(left_column,
                        Comparison::Op::EQ,
                        std::move(value));
                    *out_statement = LogicalOperation::makeOr(std::move(*out_statement),
                        std::move(new_stmt));
                }
            }
            *out_entity = std::move(left_entity);
            break;
        }
        case hsql::kOpIsNull: {
            // fmt::println("operator IsNull");
            auto child = expr->expr;
            assert_column(child);
            auto [child_column, child_entity] =
                extract_column_and_table(child, table_counts, column_to_tables, alias_map);
            // fmt::println("child_column: {}", child_column);
            // fmt::println("child_table: {}", child_table);
            *out_statement = std::make_unique<Comparison>(std::move(child_column),
                Comparison::Op::IS_NULL,
                std::monostate{});
            *out_entity    = std::move(child_entity);
            break;
        }
        default: {
            throw std::runtime_error(fmt::format("Operator type: {} not processed", op_type));
        }
        }
        break;
    }
    default: {
        throw std::runtime_error(fmt::format("Expression type: {} not processed", expr->type));
    }
    }
}

void run(const std::unordered_map<std::string, std::vector<std::string>>& column_to_tables,
    std::string_view                                                      name,
    std::string                                                           sql,
    const json&                                                           plan_json) {
    hsql::SQLParserResult sql_result;
    hsql::SQLParser::parse(sql, &sql_result);

    if (not sql_result.isValid()) {
        throw std::runtime_error(fmt::format("Error parsing SQL: {}", name));
    }

    std::unordered_map<std::string, int>              table_counts;
    AliasMapType                                      alias_map;
    JoinGraphType                                     join_graph;
    FilterMapType                                     filters;
    OutputAttrsType                                   output_attrs;
    ColumnMapType                                     column_map;
    std::vector<std::tuple<TableEntity, std::string>> column_vec;
    auto statement = (const hsql::SelectStatement*)sql_result.getStatement(0);

    auto fromTable = statement->fromTable;
    if (fromTable->type != hsql::kTableCrossProduct) {
        throw std::runtime_error("SQL not supported");
    }
    auto   fromTableList = fromTable->list;
    size_t column_count  = 0;
    for (auto table: *fromTableList) {
        if (table->type != hsql::kTableName) {
            throw std::runtime_error("SQL not supported");
        }
        auto table_itr = table_counts.find(table->name);
        if (table_itr == table_counts.end()) {
            bool _;
            std::tie(table_itr, _) = table_counts.emplace(table->name, 1);
        } else {
            ++(table_itr->second);
        }
        TableEntity entity{table->name, table_itr->second - 1};
        auto [itr, _] = column_map.emplace(entity, std::unordered_map<std::string, size_t>{});
        if (auto attr_itr = attributes_map.find(entity.table);
            attr_itr != attributes_map.end()) {
            for (auto& attr: attr_itr->second) {
                itr->second.emplace(attr.name, column_count++);
                column_vec.emplace_back(entity, attr.name);
            }
        } else {
            throw std::runtime_error(fmt::format("No table: {} in schema", entity.table));
        }
        auto alias = table->alias;
        if (alias) {
            alias_map.emplace(alias->name, entity);
        }
    }

    auto& selectList = *statement->selectList;
    for (auto* expr: selectList) {
        switch (expr->type) {
        case (hsql::kExprFunctionRef): {
            for (auto* child: *expr->exprList) {
                // fmt::println("Child: {}", child->type);
                if (child->type != hsql::kExprColumnRef) {
                    throw std::runtime_error("Complex select expressions are not supported");
                }
                auto [column, entity] =
                    extract_column_and_table(child, table_counts, column_to_tables, alias_map);
                output_attrs.emplace_back(entity, column);
            }
            break;
        }
        default: {
            throw std::runtime_error(
                fmt::format("Not supported expression type in select list: {}.", expr->type));
            break;
        }
        }
    }
    auto condition = statement->whereClause;
    DSU  join_union(column_count);
    parse_expr(condition,
        table_counts,
        column_to_tables,
        alias_map,
        column_map,
        filters,
        join_union);

    std::unordered_map<size_t, std::vector<size_t>> joined_columns;
    for (size_t i = 0; i < column_count; ++i) {
        auto set = join_union.find(i);
        if (auto itr = joined_columns.find(set); itr != joined_columns.end()) {
            itr->second.emplace_back(i);
        } else {
            joined_columns.emplace(set, std::vector<size_t>(1, i));
        }
    }

    for (auto& [set, items]: joined_columns) {
        for (size_t i = 0; i < items.size() - 1; ++i) {
            for (auto j = i + 1; j < items.size(); ++j) {
                auto& [left_entity, left_column]   = column_vec[items[i]];
                auto& [right_entity, right_column] = column_vec[items[j]];
                if (auto itr = join_graph.find(left_entity); itr != join_graph.end()) {
                    if (auto iter = itr->second.find(right_entity); iter != itr->second.end()) {
                        throw std::runtime_error(
                            "At least two conditions between a same pair of tables.");
                    }
                    itr->second.emplace(right_entity, std::tuple{left_column, right_column});
                } else {
                    std::unordered_map<TableEntity, std::tuple<std::string, std::string>>
                        adj_item;
                    adj_item.emplace(right_entity, std::tuple{left_column, right_column});
                    join_graph.emplace(left_entity, std::move(adj_item));
                }
                if (auto itr = join_graph.find(right_entity); itr != join_graph.end()) {
                    itr->second.emplace(left_entity, std::tuple{right_column, left_column});
                } else {
                    std::unordered_map<TableEntity, std::tuple<std::string, std::string>>
                        adj_item;
                    adj_item.emplace(left_entity, std::tuple{right_column, left_column});
                    join_graph.emplace(right_entity, std::move(adj_item));
                }
            }
        }
    }

    auto plan =
        load_join_pipeline(plan_json["Plan"], alias_map, filters, join_graph, output_attrs);

    auto start   = std::chrono::steady_clock::now();
    auto results = Contest::execute(plan);
    auto end     = std::chrono::steady_clock::now();

    // auto result_table = Table::from_columnar(results);
    // result_table.print();
    fmt::println("{} results: {}, {}", name, results.num_rows, results.columns.size());
    fmt::println("{}ms",
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
}

int main(int argc, char* argv[]) {
    namespace views = ranges::views;
    try {
        // column to table map
        std::unordered_map<std::string, std::vector<std::string>> column_to_tables;

        for (auto& [table_name, attributes]: attributes_map) {
            for (auto& [_, attribute_name]: attributes) {
                if (auto itr = column_to_tables.find(attribute_name);
                    itr == column_to_tables.end()) {
                    column_to_tables.emplace(attribute_name,
                        std::vector<std::string>{table_name});
                } else {
                    itr->second.push_back(table_name);
                }
            }
        }

        // load plan json
        namespace fs = std::filesystem;

        File file("plans.json", "rb");
        json query_plans = json::parse(file);
        auto names       = query_plans["names"].get<std::vector<std::string>>();
        auto plans       = query_plans["plans"];
        for (const auto& [name, plan_json]: views::zip(names, plans)) {
            if (argc < 2 or name == argv[1]) {
                auto sql_path = fs::path("job") / fmt::format("{}.sql", name);
                auto sql      = read_file(sql_path);
                run(column_to_tables, name, std::move(sql), plan_json);
            }
        }
    } catch (std::exception& e) {
        fmt::println(stderr, "Error: {}", e.what());
        exit(EXIT_FAILURE);
    }
}
