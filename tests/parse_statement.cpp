#include <print>

#include <statement.h>

// 示例用法
int main(int argc, char* argv[]) {
    try {
        std::string input = argv[1];
        // clang-format off
        // in 1a.sql
        // std::string input = "kind = 'production companies'";
        // std::string input = "info = 'top 250 rank'";
        // std::string input = "note  not like '%(as Metro-Goldwyn-Mayer Pictures)%' and (note like '%(co-production)%' or note like '%(presents)%')";
        // another example
        // std::string input = "(column_A > 10) AND (column_B LIKE 'Apple%' OR column_C IS NOT NULL)";
        // clang-format on
        auto statement = build_statement(argv[1]);
        std::println("Generated AST:\n{}", statement->pretty_print());
        return 0;
    } catch (std::exception& e) {
        std::println(stderr, "error: {}", e.what());
    }
}
