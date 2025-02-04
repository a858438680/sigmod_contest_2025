#include <catch2/catch_test_macros.hpp>

#include <spc_executor.h>
#include <statement.h>

TEST_CASE("NOT LIKE are excluded", "[like]") {
    auto attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"             },
        {DataType::INT32,   "movie_id"       },
        {DataType::INT32,   "company_id"     },
        {DataType::INT32,   "company_type_id"},
        {DataType::VARCHAR, "note"           },
    };
    auto statement = build_statement(
        "note not like '%(as Metro-Goldwyn-Mayer Pictures)%' and (note like "
        "'%(co-production)%' or note like '%(presents)%')");
    auto record1 = std::vector<Data>{
        Data{1805107},
        Data{1669115},
        Data{159},
        Data{2},
        Data{"(presents) (as Metro-Goldwyn-Mayer-Pictures)"}, // an additional hypen
    };
    auto record2 = std::vector<Data>{
        Data{1778096},
        Data{1639199},
        Data{159},
        Data{2},
        Data{"(as Metro-Goldwyn-Mayer Pictures) (presents)"},
    };
    auto result1 = statement->eval(attributes, record1);
    REQUIRE(result1 == true);
    auto result2 = statement->eval(attributes, record2);
    REQUIRE(result2 == false);
}

TEST_CASE("LIKE is case sensitive", "[like]") {
    auto attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"             },
        {DataType::INT32,   "movie_id"       },
        {DataType::INT32,   "company_id"     },
        {DataType::INT32,   "company_type_id"},
        {DataType::VARCHAR, "note"           },
    };
    auto statement = build_statement(
        "note not like '%(as Metro-Goldwyn-Mayer Pictures)%' and (note like "
        "'%(co-production)%' or note like '%(presents)%')");
    auto record1 = std::vector<Data>{
        Data{1748933},
        Data{1542786},
        Data{76409},
        Data{2},
        Data{"(Co-Production)"}, // upper case
    };
    auto result1 = statement->eval(attributes, record1);
    REQUIRE(result1 == false);
}