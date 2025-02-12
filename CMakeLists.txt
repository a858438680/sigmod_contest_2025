cmake_minimum_required(VERSION 3.29)

project(SigmodContest)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# add_compile_options()

Include(FetchContent)

FetchContent_Declare(
    Catch2
    URL https://github.com/catchorg/Catch2/archive/refs/tags/v3.8.0.tar.gz
)

FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(
    abseil
    URL https://github.com/abseil/abseil-cpp/releases/download/20240722.1/abseil-cpp-20240722.1.tar.gz
)

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
FetchContent_MakeAvailable(abseil)

FetchContent_Declare(
    re2
    URL https://github.com/google/re2/releases/download/2024-07-02/re2-2024-07-02.tar.gz
)

FetchContent_MakeAvailable(re2)

FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)

FetchContent_MakeAvailable(json)

FetchContent_Declare(
    sql-parser
    URL https://github.com/a858438680/sql-parser/archive/refs/tags/win-port.tar.gz
)
set(HSQL_ENABLE_WERROR OFF)
FetchContent_MakeAvailable(sql-parser)

add_library(contest
    STATIC
    src/build_table.cpp
    src/csv_parser.cpp
    src/statement.cpp
    src/execute.cpp
)

target_include_directories(contest PUBLIC include)
target_link_libraries(contest PUBLIC re2)
target_compile_definitions(contest PUBLIC -D_CRT_SECURE_NO_WARNINGS)

# add_executable(
#     read_plan
#     tests/read_plan.cpp
# )
# target_link_libraries(read_plan PRIVATE contest nlohmann_json::nlohmann_json)

add_executable(
    read_sql
    tests/read_sql.cpp
)
target_link_libraries(read_sql PRIVATE contest nlohmann_json::nlohmann_json sqlparser)

# add_executable(
#     join
#     tests/join.cpp
# )
# target_link_libraries(join PRIVATE contest)

# add_executable(
#     filter
#     tests/filter.cpp
# )
# target_link_libraries(filter PRIVATE contest)

# add_executable(
#     read_csv
#     tests/read_csv.cpp
# )
# target_link_libraries(read_csv PRIVATE contest)

# add_executable(
#     test_sql_parser
#     tests/test_sql_parser.cpp
# )
# target_link_libraries(test_sql_parser PRIVATE contest sqlparser)

# add_executable(
#     parse_statement
#     tests/parse_statement.cpp
# )
# target_link_libraries(parse_statement PRIVATE contest)

# add_executable(
#     statement_tests
#     tests/statement_tests.cpp
# )
# target_link_libraries(statement_tests PRIVATE Catch2::Catch2WithMain contest)

# add_executable(
#     unit_tests
#     tests/unit_tests.cpp
# )
# target_link_libraries(unit_tests PRIVATE Catch2::Catch2WithMain contest)