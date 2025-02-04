#pragma once

#include <algorithm>
#include <format>
#include <mutex>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include <re2/re2.h>

using Data = std::variant<int32_t, int64_t, double, std::string, std::monostate>;

struct Attribute;
struct Statement;
struct Comparison;
struct LogicalOperation;

// Token类型定义
struct Token {
    enum Type {
        LPAREN,
        RPAREN,
        AND,
        OR,
        NOT,
        EQ,
        NEQ,
        LT,
        GT,
        LEQ,
        GEQ,
        LIKE,
        IS,
        NULL_VAL, // 新增Token类型
        IDENTIFIER,
        LITERAL_STRING,
        LITERAL_INT,
        LITERAL_FLOAT,
        EOI
    };

    Type                                                   type;
    std::string                                            lexeme;
    std::variant<int, double, std::string, std::monostate> value; // 包含monostate
};

// AST节点定义
struct Statement {
    virtual ~Statement()                                   = default;
    virtual std::string pretty_print(int indent = 0) const = 0;
    virtual bool        eval(std::span<Attribute> attributes, std::span<Data> record) const = 0;
};

struct Comparison: Statement {
    std::string column;

    enum Op {
        EQ,
        NEQ,
        LT,
        GT,
        LEQ,
        GEQ,
        LIKE,
        NOT_LIKE,
        IS_NULL,
        IS_NOT_NULL // 新增操作类型
    };

    Op                                                     op;
    std::variant<int, double, std::string, std::monostate> value;

    Comparison(std::string                                     col,
        Op                                                     o,
        std::variant<int, double, std::string, std::monostate> val)
    : column(std::move(col))
    , op(o)
    , value(std::move(val)) {}

    std::string pretty_print(int indent) const override {
        return std::format("{:{}}{} {} {}", "", indent, column, opToString(), valueToString());
    }

    bool eval(std::span<Attribute> attributes, std::span<Data> record) const override;

private:
    std::string opToString() const {
        switch (op) {
        case EQ:          return "=";
        case NEQ:         return "!=";
        case LT:          return "<";
        case GT:          return ">";
        case LEQ:         return "<=";
        case GEQ:         return ">=";
        case LIKE:        return "LIKE";
        case NOT_LIKE:    return "NOT LIKE";
        case IS_NULL:     return "IS NULL";
        case IS_NOT_NULL: return "IS NOT NULL";
        default:          return "??";
        }
    }

    std::string valueToString() const {
        if (op == IS_NULL || op == IS_NOT_NULL) {
            return "";
        }
        return visit(
            [](auto&& arg) -> std::string {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::format("'{}'", arg);
                } else if constexpr (std::is_same_v<T, std::monostate>) {
                    return "";
                } else {
                    return std::format("{}", arg);
                }
            },
            value);
    }

    static bool like_match(const std::string& str, const std::string& pattern) {
        // 静态缓存和互斥锁
        static std::mutex cache_mutex;
        static auto       regex_cache = std::unordered_map<std::string, std::shared_ptr<RE2>>{};

        std::shared_ptr<RE2> re;

        // 检查缓存
        {
            std::lock_guard<std::mutex> lock(cache_mutex);
            auto                        it = regex_cache.find(pattern);
            if (it != regex_cache.end()) {
                re = it->second;
            }
        }

        // 未命中缓存则编译并缓存
        if (!re) {
            // 将 SQL 通配符转换为正则表达式字符串
            std::string regex_str;
            for (char c: pattern) {
                if (c == '%') {
                    regex_str += ".*";
                } else if (c == '_') {
                    regex_str += '.';
                } else {
                    // 转义正则特殊字符
                    if (c == '\\' || c == '.' || c == '^' || c == '$' || c == '|' || c == '?'
                        || c == '*' || c == '+' || c == '(' || c == ')' || c == '[' || c == ']'
                        || c == '{' || c == '}' || c == ' ') {
                        regex_str += '\\';
                    }
                    regex_str += c;
                }
            }

            RE2::Options options;
            // options.set_case_sensitive(false); // 忽略大小写

            auto new_re = std::make_shared<RE2>(regex_str, options);
            if (!new_re->ok()) {
                return false; // 正则表达式无效
            }

            // 双检锁避免重复插入
            std::lock_guard<std::mutex> lock(cache_mutex);
            if (auto itr = regex_cache.find(pattern); itr == regex_cache.end()) {
                auto [it, _] = regex_cache.emplace(pattern, new_re);
                re           = it->second;
            } else {
                re = itr->second;
            }
        }

        // 执行完全匹配
        return RE2::FullMatch(str, *re);
    }

    static std::optional<double> get_numeric_value(const Data& data) {
        if (auto* i32 = std::get_if<int32_t>(&data)) {
            return *i32;
        } else if (auto* i64 = std::get_if<int64_t>(&data)) {
            return static_cast<double>(*i64);
        } else if (auto* d = std::get_if<double>(&data)) {
            return *d;
        } else {
            return std::nullopt;
        }
    }

    static std::optional<double> get_numeric_value(
        const std::variant<int, double, std::string, std::monostate>& value) {
        if (auto* i = std::get_if<int>(&value)) {
            return *i;
        } else if (auto* d = std::get_if<double>(&value)) {
            return *d;
        } else {
            return std::nullopt;
        }
    }
};

struct LogicalOperation: Statement {
    enum Type {
        AND,
        OR,
        NOT
    };

    Type                                    op_type;
    std::vector<std::unique_ptr<Statement>> children;

    static std::unique_ptr<LogicalOperation> makeAnd(std::unique_ptr<Statement> l,
        std::unique_ptr<Statement>                                              r) {
        auto node     = std::make_unique<LogicalOperation>();
        node->op_type = AND;
        node->children.push_back(std::move(l));
        node->children.push_back(std::move(r));
        return node;
    }

    static std::unique_ptr<LogicalOperation> makeOr(std::unique_ptr<Statement> l,
        std::unique_ptr<Statement>                                             r) {
        auto node     = std::make_unique<LogicalOperation>();
        node->op_type = OR;
        node->children.push_back(std::move(l));
        node->children.push_back(std::move(r));
        return node;
    }

    static std::unique_ptr<LogicalOperation> makeNot(std::unique_ptr<Statement> child) {
        auto node     = std::make_unique<LogicalOperation>();
        node->op_type = NOT;
        node->children.push_back(std::move(child));
        return node;
    }

    std::string pretty_print(int indent) const override {
        std::string op_str = [this] {
            switch (op_type) {
            case AND: return "AND";
            case OR:  return "OR";
            case NOT: return "NOT";
            default:  return "UNKNOWN";
            }
        }();

        std::string result = std::format("{:{}}[{}]\n", "", indent, op_str);

        for (auto& child: children) {
            result += child->pretty_print(indent + 2) + "\n";
        }

        if (!children.empty()) {
            result.pop_back();
        }
        return result;
    }

    bool eval(std::span<Attribute> attributes, std::span<Data> record) const override;
};

// 词法分析器
class Lexer {
    std::string input;
    size_t      pos = 0;

    char peek() const { return pos < input.size() ? input[pos] : '\0'; }

    void consume() {
        if (pos < input.size()) {
            ++pos;
        }
    }

    std::string readIdentifier() {
        size_t start = pos;
        while (isalnum(peek()) || peek() == '_') {
            consume();
        }
        return input.substr(start, pos - start);
    }

    std::string readString() {
        consume(); // 跳过起始'
        size_t start = pos;
        while (peek() != '\'') {
            if (peek() == '\0') {
                throw std::runtime_error("Unclosed std::string");
            }
            consume();
        }
        std::string str = input.substr(start, pos - start);
        consume(); // 跳过结束'
        return str;
    }

    struct NumberInfo {
        std::string lexeme;
        bool        is_float;
        int         ivalue;
        double      dvalue;
    };

    NumberInfo readNumber() {
        size_t start   = pos;
        bool   has_dot = false;
        while (isdigit(peek()) || peek() == '.') {
            if (peek() == '.') {
                if (has_dot) {
                    throw std::runtime_error("Invalid number");
                }
                has_dot = true;
            }
            consume();
        }
        std::string lexeme = input.substr(start, pos - start);

        try {
            if (has_dot) {
                return {lexeme, true, 0, stod(lexeme)};
            } else {
                return {lexeme, false, stoi(lexeme), 0.0};
            }
        } catch (...) {
            throw std::runtime_error("Invalid number format");
        }
    }

public:
    Lexer(std::string input)
    : input(std::move(input)) {}

    std::vector<Token> tokenize() {
        std::vector<Token> tokens;
        while (pos < input.size()) {
            char c = peek();
            if (isspace(c)) {
                consume();
                continue;
            }

            if (c == '(') {
                tokens.push_back({Token::LPAREN, "(", {}});
                consume();
            } else if (c == ')') {
                tokens.push_back({Token::RPAREN, ")", {}});
                consume();
            } else if (isalpha(c)) {
                std::string ident = readIdentifier();
                std::string upper_ident;
                transform(ident.begin(), ident.end(), back_inserter(upper_ident), ::toupper);

                if (upper_ident == "AND") {
                    tokens.push_back({Token::AND, "AND", {}});
                } else if (upper_ident == "OR") {
                    tokens.push_back({Token::OR, "OR", {}});
                } else if (upper_ident == "NOT") {
                    tokens.push_back({Token::NOT, "NOT", {}});
                } else if (upper_ident == "LIKE") {
                    tokens.push_back({Token::LIKE, "LIKE", {}});
                } else if (upper_ident == "IS") {
                    tokens.push_back({Token::IS, "IS", {}});
                } else if (upper_ident == "NULL") {
                    tokens.push_back({Token::NULL_VAL, "NULL", std::monostate{}});
                } else {
                    tokens.push_back({Token::IDENTIFIER, ident, {}});
                }
            } else if (c == '\'') {
                std::string str = readString();
                tokens.push_back({Token::LITERAL_STRING, str, str});
            } else if (isdigit(c) || c == '.') {
                auto num = readNumber();
                if (num.is_float) {
                    tokens.push_back({Token::LITERAL_FLOAT, num.lexeme, num.dvalue});
                } else {
                    tokens.push_back({Token::LITERAL_INT, num.lexeme, num.ivalue});
                }
            } else if (c == '=') {
                tokens.push_back({Token::EQ, "=", {}});
                consume();
            } else if (c == '<') {
                consume();
                if (peek() == '=') {
                    tokens.push_back({Token::LEQ, "<=", {}});
                    consume();
                } else {
                    tokens.push_back({Token::LT, "<", {}});
                }
            } else if (c == '>') {
                consume();
                if (peek() == '=') {
                    tokens.push_back({Token::GEQ, ">=", {}});
                    consume();
                } else {
                    tokens.push_back({Token::GT, ">", {}});
                }
            } else if (c == '!') {
                consume();
                if (peek() == '=') {
                    tokens.push_back({Token::NEQ, "!=", {}});
                    consume();
                } else {
                    throw std::runtime_error("Invalid operator !");
                }
            } else {
                throw std::runtime_error(std::string("Unexpected character: ") + c);
            }
        }
        tokens.push_back({Token::EOI, "", {}});
        return tokens;
    }
};

// 语法解析器
class Parser {
    std::vector<Token> tokens;
    size_t             pos = 0;

    const Token& current() const { return tokens[pos]; }

    void consume() { pos = std::min(pos + 1, tokens.size()); }

    Comparison::Op convertCompareOp(Token::Type type) {
        switch (type) {
        case Token::EQ:  return Comparison::EQ;
        case Token::NEQ: return Comparison::NEQ;
        case Token::LT:  return Comparison::LT;
        case Token::GT:  return Comparison::GT;
        case Token::LEQ: return Comparison::LEQ;
        case Token::GEQ: return Comparison::GEQ;
        default:         throw std::runtime_error("Invalid comparison operator");
        }
    }

    std::unique_ptr<Statement>  parseOr();
    std::unique_ptr<Statement>  parseAnd();
    std::unique_ptr<Statement>  parseNot();
    std::unique_ptr<Statement>  parseFactor();
    std::unique_ptr<Comparison> parseComparison();

public:
    Parser(std::vector<Token> tokens)
    : tokens(std::move(tokens)) {}

    std::unique_ptr<Statement> parse() { return parseOr(); }
};

std::unique_ptr<Statement> build_statement(std::string input);
