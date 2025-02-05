/*
 * Copyright 2025 Matthias Boehm, TU Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// API of the SIGMOD 2025 Programming Contest,
// See https://sigmod-contest-2025.github.io/index.html

#include <filesystem>
#include <print>
#include <ranges>
#include <span>

#include <cstdint>

#include <statement.h>

constexpr int MAX_ATTR_NAME_LEN = 32;

// error codes for API functions
enum class ErrCode {
    SUCCESS,
    OUT_OF_MEMORY,
    FAILURE
};

// supported attribute data types
enum class DataType {
    INT32,   // 4-byte integer
    INT64,   // 8-byte integer
    FP64,    // 8-byte floating point
    VARCHAR, // string of arbitary length
};

// attribute meta data
struct Attribute {
    DataType type;
    char     name[MAX_ATTR_NAME_LEN + 1];
};

struct PlanNode;

struct Table {
public:
    friend struct PlanNode;

    static Table from_csv(std::span<Attribute> attributes,
        const std::filesystem::path&           path,
        Statement*                             filter,
        bool                                   header = false);

    const std::vector<std::vector<Data>>& table() { return data_; }

    const std::vector<Attribute>& attributes() { return attributes_; }

    size_t number_rows() {
        return this->data_.size();
    }

    size_t number_cols() {
        return this->attributes_.size();
    }

    void print() {
        namespace ranges = std::ranges;
        namespace views  = std::views;

        // 定义转义字符串的lambda
        auto escape_string = [](const std::string& s) {
            std::string escaped;
            for (char c: s) {
                switch (c) {
                case '"':  escaped += "\\\""; break; // 转义双引号
                case '\\': escaped += "\\\\"; break; // 转义反斜杠
                case '\n': escaped += "\\n"; break;  // 转义换行符
                case '\r': escaped += "\\r"; break;  // 转义回车符
                case '\t': escaped += "\\t"; break;  // 转义制表符
                default:   escaped += c; break;
                }
            }
            return escaped;
        };

        for (auto& record: this->data_) {
            auto line = record
                      | views::transform([&escape_string](const Data& field) -> std::string {
                            return std::visit(
                                [&escape_string](const auto& arg) {
                                    using T = std::decay_t<decltype(arg)>;
                                    using namespace std::string_literals;
                                    if constexpr (std::is_same_v<T, std::monostate>) {
                                        return "NULL"s;
                                    } else if constexpr (std::is_same_v<T, int32_t>
                                                         || std::is_same_v<T, int64_t>
                                                         || std::is_same_v<T, double>) {
                                        return std::format("{}", arg);
                                    } else if constexpr (std::is_same_v<T, std::string>) {
                                        return std::format("\"{}\"", escape_string(arg));
                                        // return std::format("{}", arg);
                                    }
                                },
                                field);
                        })
                      | views::join_with('|') | ranges::to<std::string>();
            std::println("{}", line);
        }
    }

private:
    std::vector<Attribute>         attributes_;
    std::vector<std::vector<Data>> data_;

    void set_attributes(std::span<Attribute> attributes) {
        this->attributes_.clear();
        for (auto& attr: attributes) {
            this->attributes_.push_back(attr);
        }
    }
};

enum class NodeType {
    NestedLoopJoin,
    HashJoin,
    SortMergeJoin,
    Scan,
};

// query execution plan of equi joins
// inner nodes: left and right defined, base table undefined
// leaf nodes: base table defined, left and right undefined
struct PlanNode {
    NodeType         type;
    bool             build_left;
    PlanNode*        left;
    PlanNode*        right;
    const Attribute* leftAttr;
    const Attribute* rightAttr;
    Table*           baseTable;

    Table execute() {
        auto  results    = this->execute_impl();
        auto  attributes = this->attributes();
        Table table;
        for (auto pattr: attributes) {
            table.attributes_.emplace_back(*pattr);
        }
        table.data_ = std::move(results);
        return table;
    }

private:
    std::vector<std::vector<Data>> execute_impl() {
        switch (type) {
        case NodeType::Scan:           return execute_scan();
        case NodeType::HashJoin:       return execute_hash_join();
        case NodeType::NestedLoopJoin: return execute_nested_loop_join();
        case NodeType::SortMergeJoin:  return execute_sort_merge_join();
        }
    }

    std::vector<const Attribute*> attributes() {
        if (type == NodeType::Scan) {
            std::vector<const Attribute*> attrs;
            const auto&                   table_attrs = baseTable->attributes();
            attrs.reserve(table_attrs.size());
            for (const auto& attr: table_attrs) {
                attrs.push_back(&attr);
            }
            return attrs;
        } else {
            auto left_attrs  = left->attributes();
            auto right_attrs = right->attributes();
            left_attrs.insert(left_attrs.end(), right_attrs.begin(), right_attrs.end());
            return left_attrs;
        }
    }

    std::vector<std::vector<Data>> execute_nested_loop_join() {
        auto left             = this->left->execute_impl();
        auto right            = this->right->execute_impl();
        auto left_attributes  = this->left->attributes();
        auto right_attributes = this->right->attributes();
        auto new_size         = left_attributes.size() + right_attributes.size();
        auto left_col =
            std::find(left_attributes.begin(), left_attributes.end(), this->leftAttr)
            - left_attributes.begin();
        auto right_col =
            std::find(right_attributes.begin(), right_attributes.end(), this->rightAttr)
            - right_attributes.begin();
        std::vector<std::vector<Data>> results;
        auto                           join_algorithm =
            [&left, &right, &results, left_col, right_col, new_size]<class T>() {
                for (auto& left_record: left) {
                    auto left_key = std::get<T>(left_record[left_col]);
                    for (auto& right_record: right) {
                        auto right_key = std::get<T>(right_record[right_col]);
                        if (left_key == right_key) {
                            std::vector<Data> new_record;
                            new_record.reserve(new_size);
                            new_record.insert(new_record.end(),
                                left_record.begin(),
                                left_record.end());
                            new_record.insert(new_record.end(),
                                right_record.begin(),
                                right_record.end());
                            results.emplace_back(std::move(new_record));
                        }
                    }
                }
            };
        switch (this->leftAttr->type) {
        case DataType::INT32:   join_algorithm.template operator()<int32_t>(); break;
        case DataType::INT64:   join_algorithm.template operator()<int64_t>(); break;
        case DataType::FP64:    join_algorithm.template operator()<double>(); break;
        case DataType::VARCHAR: join_algorithm.template operator()<std::string>(); break;
        }
        return results;
    }

    std::vector<std::vector<Data>> execute_hash_join() {
        auto left             = this->left->execute_impl();
        auto right            = this->right->execute_impl();
        auto left_attributes  = this->left->attributes();
        auto right_attributes = this->right->attributes();
        auto new_size         = left_attributes.size() + right_attributes.size();
        auto left_col =
            std::find(left_attributes.begin(), left_attributes.end(), this->leftAttr)
            - left_attributes.begin();
        auto right_col =
            std::find(right_attributes.begin(), right_attributes.end(), this->rightAttr)
            - right_attributes.begin();
        std::vector<std::vector<Data>> results;
        namespace views = std::views;
        auto join_algorithm =
            [&left, &right, &results, left_col, right_col, new_size]<class T>() {
                std::unordered_map<T, std::vector<size_t>> hash_table;
                for (auto&& [idx, record]: left | views::enumerate) {
                    auto key = std::get<T>(record[left_col]);
                    if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                        hash_table.emplace(key, std::vector<size_t>(1, idx));
                    } else {
                        itr->second.push_back(idx);
                    }
                }
                for (auto& record: right) {
                    auto key = std::get<T>(record[right_col]);
                    if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                        for (auto left_idx: itr->second) {
                            std::vector<Data> new_record;
                            new_record.reserve(new_size);
                            new_record.insert(new_record.end(),
                                left[left_idx].begin(),
                                left[left_idx].end());
                            new_record.insert(new_record.end(), record.begin(), record.end());
                            results.emplace_back(std::move(new_record));
                        }
                    }
                }
            };
        switch (this->leftAttr->type) {
        case DataType::INT32:   join_algorithm.template operator()<int32_t>(); break;
        case DataType::INT64:   join_algorithm.template operator()<int64_t>(); break;
        case DataType::FP64:    join_algorithm.template operator()<double>(); break;
        case DataType::VARCHAR: join_algorithm.template operator()<std::string>(); break;
        }
        return results;
    }

    std::vector<std::vector<Data>> execute_sort_merge_join() {
        auto left             = this->left->execute_impl();
        auto right            = this->right->execute_impl();
        auto left_attributes  = this->left->attributes();
        auto right_attributes = this->right->attributes();
        auto new_size         = left_attributes.size() + right_attributes.size();
        auto left_col =
            std::find(left_attributes.begin(), left_attributes.end(), this->leftAttr)
            - left_attributes.begin();
        auto right_col =
            std::find(right_attributes.begin(), right_attributes.end(), this->rightAttr)
            - right_attributes.begin();
        std::vector<std::vector<Data>> results;
        auto                           join_algorithm = [&left,
                                  &right,
                                  &results,
                                  left_col,
                                  right_col,
                                  new_size]<class T>() {
            std::sort(left.begin(), left.end(), [left_col](const auto& lhs, const auto& rhs) {
                auto lhs_key = std::get<T>(lhs[left_col]);
                auto rhs_key = std::get<T>(lhs[left_col]);
                return lhs_key < rhs_key;
            });
            std::sort(right.begin(),
                right.end(),
                [right_col](const auto& lhs, const auto& rhs) {
                    auto lhs_key = std::get<T>(lhs[right_col]);
                    auto rhs_key = std::get<T>(lhs[right_col]);
                    return lhs_key < rhs_key;
                });

            size_t i = 0;
            size_t j = 0;

            while (i < left.size() && j < right.size()) {
                auto left_val  = std::get<T>(left[i][left_col]);
                auto right_val = std::get<T>(right[j][right_col]);

                if (left_val < right_val) {
                    ++i;
                } else if (right_val < left_val) {
                    ++j;
                } else {
                    // Find all rows with equal keys in left
                    size_t i_end = i;
                    while (
                        i_end < left.size() && std::get<T>(left[i_end][left_col]) == left_val) {
                        ++i_end;
                    }

                    // Find all rows with equal keys in right
                    size_t j_end = j;
                    while (j_end < right.size()
                           && std::get<T>(right[j_end][right_col]) == right_val) {
                        ++j_end;
                    }

                    // Generate cross product of matching rows
                    for (size_t a = i; a < i_end; ++a) {
                        for (size_t b = j; b < j_end; ++b) {
                            std::vector<Data> new_record;
                            new_record.reserve(new_size);
                            new_record.insert(new_record.end(), left[a].begin(), left[a].end());
                            new_record.insert(new_record.end(),
                                right[b].begin(),
                                right[b].end());
                            results.emplace_back(std::move(new_record));
                        }
                    }

                    // Move to next potential keys
                    i = i_end;
                    j = j_end;
                }
            }
        };
        switch (this->leftAttr->type) {
        case DataType::INT32:   join_algorithm.template operator()<int32_t>(); break;
        case DataType::INT64:   join_algorithm.template operator()<int64_t>(); break;
        case DataType::FP64:    join_algorithm.template operator()<double>(); break;
        case DataType::VARCHAR: join_algorithm.template operator()<std::string>(); break;
        }
        return results;
    }

    std::vector<std::vector<Data>> execute_scan() { return this->baseTable->table(); }
};

/**
 * Execute the given query execution plan and produce the
 * output table in PAX page layout.
 *
 * @param plan the query execution plan, including pointers to the base tables
 * @param output the result of the query execution
 * @return ErrCode
 * SUCCESS if successfully executed the query execution plan.
 * OUT_OF_MEMORY if memory allocation errors occured during plan execution.
 * FAILURE if execution failed for some other reason.
 */
ErrCode execute(PlanNode* plan, Table** output);
