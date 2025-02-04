#include <print>

#include <spc_executor.h>

int main(int argc, char* argv[]) {
    // auto attributes = std::vector<Attribute>{
    //     Attribute{DataType::INT32,       "id"  },
    //     Attribute{DataType::VARCHAR_255, "kind"},
    // };
    // auto statement = build_statement("kind = 'production companies'");
    // auto table     = Table::from_csv(attributes, "imdb/company_type.csv", statement.get());

    // CREATE TABLE movie_companies (
    //     id integer NOT NULL PRIMARY KEY,
    //     movie_id integer NOT NULL,
    //     company_id integer NOT NULL,
    //     company_type_id integer NOT NULL,
    //     note character varying
    // );
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
    auto table = Table::from_csv(attributes, "imdb/movie_companies.csv", statement.get());
    table.print();
}
