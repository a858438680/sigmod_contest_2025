#include <statement.h>
#include <plan.h>

std::unique_ptr<Statement> Parser::parseOr() {
    auto left = parseAnd();
    while (current().type == Token::OR) {
        consume();
        auto right = parseAnd();
        left       = LogicalOperation::makeOr(std::move(left), std::move(right));
    }
    return left;
}

std::unique_ptr<Statement> Parser::parseAnd() {
    auto left = parseNot();
    while (current().type == Token::AND) {
        consume();
        auto right = parseNot();
        left       = LogicalOperation::makeAnd(std::move(left), std::move(right));
    }
    return left;
}

std::unique_ptr<Statement> Parser::parseNot() {
    if (current().type == Token::NOT) {
        consume();
        auto child = parseNot();
        return LogicalOperation::makeNot(std::move(child));
    }
    return parseFactor();
}

std::unique_ptr<Statement> Parser::parseFactor() {
    if (current().type == Token::LPAREN) {
        consume();
        auto expr = parseOr();
        if (current().type != Token::RPAREN) {
            throw std::runtime_error("Expected ')'");
        }
        consume();
        return expr;
    }
    return parseComparison();
}

std::unique_ptr<Comparison> Parser::parseComparison() {
    if (current().type != Token::IDENTIFIER) {
        throw std::runtime_error("Expected identifier");
    }
    std::string column = current().lexeme;
    consume();

    Comparison::Op                                         op;
    std::variant<int64_t, double, std::string, std::monostate> value;

    // process operand
    if (current().type >= Token::EQ && current().type <= Token::GEQ) {
        op = convertCompareOp(current().type);
        consume();
    } else if (current().type == Token::LIKE) {
        op = Comparison::LIKE;
        consume();
    } else if (current().type == Token::NOT) {
        consume();
        if (current().type != Token::LIKE) {
            throw std::runtime_error("Expected LIKE after NOT");
        }
        op = Comparison::NOT_LIKE;
        consume();
    } else if (current().type == Token::IS) {
        consume();
        if (current().type == Token::NOT) {
            consume();
            if (current().type != Token::NULL_VAL) {
                throw std::runtime_error("Expected NULL after IS NOT");
            }
            op = Comparison::IS_NOT_NULL;
            consume();
        } else if (current().type == Token::NULL_VAL) {
            op = Comparison::IS_NULL;
            consume();
        } else {
            throw std::runtime_error("Expected NULL after IS");
        }
        value = std::monostate{};
    } else {
        throw std::runtime_error("Invalid comparison operator");
    }

    // processs operand
    if (op != Comparison::IS_NULL && op != Comparison::IS_NOT_NULL) {
        switch (current().type) {
        case Token::LITERAL_INT:    value = get<int64_t>(current().value); break;
        case Token::LITERAL_FLOAT:  value = get<double>(current().value); break;
        case Token::LITERAL_STRING: value = get<std::string>(current().value); break;
        default:                    throw std::runtime_error("Expected literal value");
        }
        consume();
    }

    return std::make_unique<Comparison>(std::move(column), op, std::move(value));
}

std::unique_ptr<Statement> build_statement(std::string input) {
    Lexer  lexer(std::move(input));
    auto   tokens = lexer.tokenize();
    Parser parser(std::move(tokens));
    auto   statement = parser.parse();
    return statement;
}

bool Comparison::eval(std::span<Attribute> attributes, std::span<Data> record) const {
    size_t index = attributes.size();
    for (size_t i = 0; i < attributes.size(); ++i) {
        if (attributes[i].name == column) {
            index = i;
            break;
        }
    }
    if (index >= attributes.size()) {
        return false;
    }

    const Data& record_data = record[index];
    const auto& comp_value = value;

    switch (op) {
        case IS_NULL:
            return std::holds_alternative<std::monostate>(record_data);
        case IS_NOT_NULL:
            return !std::holds_alternative<std::monostate>(record_data);
        default:
            break;
    }

    if (op == LIKE || op == NOT_LIKE) {
        const std::string* record_str = std::get_if<std::string>(&record_data);
        const std::string* comp_str = std::get_if<std::string>(&comp_value);
        if (!record_str || !comp_str) {
            return false;
        }
        bool match = like_match(*record_str, *comp_str);
        return (op == LIKE) ? match : !match;
    } else {
        auto record_num = get_numeric_value(record_data);
        auto comp_num = get_numeric_value(comp_value);
        if (record_num.has_value() && comp_num.has_value()) {
            switch (op) {
                case EQ:  return *record_num == *comp_num;
                case NEQ: return *record_num != *comp_num;
                case LT:  return *record_num < *comp_num;
                case GT:  return *record_num > *comp_num;
                case LEQ: return *record_num <= *comp_num;
                case GEQ: return *record_num >= *comp_num;
                default:  return false;
            }
        } else {
            const std::string* record_str = std::get_if<std::string>(&record_data);
            const std::string* comp_str = std::get_if<std::string>(&comp_value);
            if (record_str && comp_str) {
                switch (op) {
                    case EQ:  return *record_str == *comp_str;
                    case NEQ: return *record_str != *comp_str;
                    case LT:  return *record_str < *comp_str;
                    case GT:  return *record_str > *comp_str;
                    case LEQ: return *record_str <= *comp_str;
                    case GEQ: return *record_str >= *comp_str;
                    default:  return false;
                }
            } else {
                return false;
            }
        }
    }
}

bool LogicalOperation::eval(std::span<Attribute> attributes, std::span<Data> record) const {
    switch (op_type) {
        case AND: {
            for (const auto& child : children) {
                if (!child->eval(attributes, record)) {
                    return false;
                }
            }
            return true;
        }
        case OR: {
            for (const auto& child : children) {
                if (child->eval(attributes, record)) {
                    return true;
                }
            }
            return false;
        }
        case NOT: {
            if (children.size() != 1) {
                return false;
            }
            return !children[0]->eval(attributes, record);
        }
        default:
            return false;
    }
}