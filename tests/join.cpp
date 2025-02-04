#include <print>

#include <spc_executor.h>

int main(int argc, char* argv[]) {
    auto ct_attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"  },
        {DataType::VARCHAR, "kind"},
    };
    auto ct_statement = build_statement("kind = 'production companies'");
    auto ct_table = Table::from_csv(ct_attributes, "imdb/company_type.csv", ct_statement.get());

    auto it_attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"  },
        {DataType::VARCHAR, "info"},
    };
    auto it_statement = build_statement("info = 'top 250 rank'");
    auto it_table = Table::from_csv(it_attributes, "imdb/info_type.csv", it_statement.get());

    auto mc_attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"             },
        {DataType::INT32,   "movie_id"       },
        {DataType::INT32,   "company_id"     },
        {DataType::INT32,   "company_type_id"},
        {DataType::VARCHAR, "note"           },
    };
    auto mc_statement = build_statement(
        "note not like '%(as Metro-Goldwyn-Mayer Pictures)%' and (note like "
        "'%(co-production)%' or note like '%(presents)%')");
    auto mc_table = Table::from_csv(mc_attributes, "imdb/movie_companies.csv", mc_statement.get());

    auto mi_idx_attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"          },
        {DataType::INT32,   "movie_id"    },
        {DataType::INT32,   "info_type_id"},
        {DataType::VARCHAR, "info"        },
        {DataType::VARCHAR, "note"        },
    };
    auto mi_idx_statement = std::shared_ptr<Statement>();
    auto mi_idx_table =
        Table::from_csv(mi_idx_attributes, "imdb/movie_info_idx.csv", mi_idx_statement.get());

    auto t_attributes = std::vector<Attribute>{
        {DataType::INT32,   "id"             },
        {DataType::VARCHAR, "title"          },
        {DataType::VARCHAR, "imdb_index"     },
        {DataType::INT32,   "kind_id"        },
        {DataType::INT32,   "production_year"},
        {DataType::INT32,   "imdb_id"        },
        {DataType::VARCHAR, "phonetic_code"  },
        {DataType::INT32,   "episode_of_id"  },
        {DataType::INT32,   "season_nr"      },
        {DataType::INT32,   "episode_nr"     },
        {DataType::VARCHAR, "series_years"   },
        {DataType::VARCHAR, "md5sum"         },
    };
    auto t_statement = std::shared_ptr<Statement>();
    auto t_table     = Table::from_csv(t_attributes, "imdb/title.csv", t_statement.get());

    PlanNode ct_scan = PlanNode{
        .type = NodeType::Scan,
        .left = nullptr,
        .right = nullptr,
        .leftAttr = nullptr,
        .rightAttr = nullptr,
        .baseTable = &ct_table,
    };

    PlanNode it_scan = PlanNode{
        .type = NodeType::Scan,
        .left = nullptr,
        .right = nullptr,
        .leftAttr = nullptr,
        .rightAttr = nullptr,
        .baseTable = &it_table,
    };

    PlanNode mc_scan = PlanNode{
        .type = NodeType::Scan,
        .left = nullptr,
        .right = nullptr,
        .leftAttr = nullptr,
        .rightAttr = nullptr,
        .baseTable = &mc_table,
    };

    PlanNode mi_idx_scan = PlanNode{
        .type = NodeType::Scan,
        .left = nullptr,
        .right = nullptr,
        .leftAttr = nullptr,
        .rightAttr = nullptr,
        .baseTable = &mi_idx_table,
    };

    PlanNode t_scan = PlanNode{
        .type = NodeType::Scan,
        .left = nullptr,
        .right = nullptr,
        .leftAttr = nullptr,
        .rightAttr = nullptr,
        .baseTable = &t_table,
    };

    PlanNode join_1 = PlanNode{
        .type = NodeType::HashJoin,
        .build_left = false,
        .left = &mi_idx_scan,
        .right = &it_scan,
        .leftAttr = &mi_idx_table.attributes()[2],
        .rightAttr = &it_table.attributes()[0],
        .baseTable = nullptr,
    };

    PlanNode join_2 = PlanNode{
        .type = NodeType::HashJoin,
        .build_left = false,
        .left = &mc_scan,
        .right = &join_1,
        .leftAttr = &mc_table.attributes()[1],
        .rightAttr = &mi_idx_table.attributes()[1],
        .baseTable = nullptr,
    };

    PlanNode join_3 = PlanNode{
        .type = NodeType::HashJoin,
        .build_left = false,
        .left = &join_2,
        .right = &ct_scan,
        .leftAttr = &mc_table.attributes()[3],
        .rightAttr = &ct_table.attributes()[0],
        .baseTable = nullptr,
    };

    PlanNode join_4 = PlanNode{
        .type = NodeType::NestedLoopJoin,
        .left = &join_3,
        .right = &t_scan,
        .leftAttr = &mc_table.attributes()[1],
        .rightAttr = &t_table.attributes()[0],
        .baseTable = nullptr,
    };

    auto start = std::chrono::steady_clock::now();
    auto results = join_4.execute();
    auto stop = std::chrono::steady_clock::now();
    results.print();
    std::println("{}", stop - start);
}
