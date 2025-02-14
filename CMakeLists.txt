cmake_minimum_required(VERSION 3.29)

project(SigmodContest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
    URL https://github.com/a858438680/sql-parser/archive/refs/tags/win-port-1.tar.gz
)
set(HSQL_ENABLE_WERROR OFF)
FetchContent_MakeAvailable(sql-parser)

FetchContent_Declare(
    range-v3
    URL https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz
)

FetchContent_MakeAvailable(range-v3)

FetchContent_Declare(
    fmtlib
    URL https://github.com/fmtlib/fmt/releases/download/11.1.3/fmt-11.1.3.zip
)

FetchContent_MakeAvailable(fmtlib)

FetchContent_Declare(
    duckdb
    URL https://github.com/duckdb/duckdb/archive/refs/tags/v1.2.0.tar.gz
)
FetchContent_MakeAvailable(duckdb)

add_library(contest
    STATIC
    src/build_table.cpp
    src/csv_parser.cpp
    src/statement.cpp
    src/execute.cpp
)

target_include_directories(contest PUBLIC include)
target_link_libraries(contest PUBLIC re2 fmt range-v3)
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
target_link_libraries(read_sql PRIVATE contest nlohmann_json::nlohmann_json sqlparser duckdb)

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
