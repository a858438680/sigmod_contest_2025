#pragma once

#include <filesystem>
#include <print>
#include <ranges>

#include <attribute.h>
#include <plan.h>
#include <statement.h>

struct Table {
public:
    Table() = default;

    Table(std::vector<std::vector<Data>> data, std::vector<DataType> types)
    : types_(types)
    , data_(data) {}

    static Table from_csv(std::span<Attribute> attributes,
        const std::filesystem::path&           path,
        Statement*                             filter,
        bool                                   header = false);

    static Table from_columnar(const ColumnarTable& input);

    ColumnarTable to_columnar() const;

    const std::vector<std::vector<Data>>& table() const { return data_; }

    const std::vector<DataType>& types() const { return types_; }

    size_t number_rows() const { return this->data_.size(); }

    size_t number_cols() const { return this->types_.size(); }

    void print() const {
        namespace ranges = std::ranges;
        namespace views  = std::views;

        // 定义转义字符串的lambda
        auto escape_string = [](const std::string& s) {
            std::string escaped;
            for (char c: s) {
                switch (c) {
                case '"':  escaped += "\\\""; break;
                case '\\': escaped += "\\\\"; break;
                case '\n': escaped += "\\n"; break;
                case '\r': escaped += "\\r"; break;
                case '\t': escaped += "\\t"; break;
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
    std::vector<DataType>          types_;
    std::vector<std::vector<Data>> data_;

    void set_attributes(std::span<Attribute> attributes) {
        this->types_.clear();
        for (auto& attr: attributes) {
            this->types_.push_back(attr.type);
        }
    }
};
