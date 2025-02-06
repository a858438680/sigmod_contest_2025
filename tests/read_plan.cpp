#include <memory>
#include <nlohmann/json.hpp>
#include <unordered_map>

#include <spc_executor.h>

using json = nlohmann::json;

std::unordered_map<std::string, std::vector<Attribute>> attributes_map = {
    {"aka_name",
     {{DataType::INT32, "id"},
            {DataType::INT32, "person_id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::VARCHAR, "name_pcode_cf"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "surname_pcode"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"aka_title",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::VARCHAR, "title"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "kind_id"},
            {DataType::INT32, "production_year"},
            {DataType::VARCHAR, "phonetic_code"},
            {DataType::INT32, "episode_of_id"},
            {DataType::INT32, "season_nr"},
            {DataType::INT32, "episode_nr"},
            {DataType::VARCHAR, "note"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"cast_info",
     {{DataType::INT32, "id"},
            {DataType::INT32, "person_id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "person_role_id"},
            {DataType::VARCHAR, "note"},
            {DataType::INT32, "nr_order"},
            {DataType::INT32, "role_id"}}                                     },
    {"char_name",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "surname_pcode"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"comp_cast_type",  {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"company_name",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "country_code"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "name_pcode_sf"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"company_type",    {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"complete_cast",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "subject_id"},
            {DataType::INT32, "status_id"}}                                   },
    {"info_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "info"}}},
    {"keyword",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "keyword"},
            {DataType::VARCHAR, "phonetic_code"}}                             },
    {"kind_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"link_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "link"}}},
    {"movie_companies",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "company_id"},
            {DataType::INT32, "company_type_id"},
            {DataType::VARCHAR, "note"}}                                      },
    {"movie_info_idx",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "info_type_id"},
            {DataType::VARCHAR, "info"},
            {DataType::VARCHAR, "note"}}                                      },
    {"movie_keyword",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "keyword_id"}}                                  },
    {"movie_link",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "linked_movie_id"},
            {DataType::INT32, "link_type_id"}}                                },
    {"name",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "name"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "gender"},
            {DataType::VARCHAR, "name_pcode_cf"},
            {DataType::VARCHAR, "name_pcode_nf"},
            {DataType::VARCHAR, "surname_pcode"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"role_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "role"}}},
    {"title",
     {{DataType::INT32, "id"},
            {DataType::VARCHAR, "title"},
            {DataType::VARCHAR, "imdb_index"},
            {DataType::INT32, "kind_id"},
            {DataType::INT32, "production_year"},
            {DataType::INT32, "imdb_id"},
            {DataType::VARCHAR, "phonetic_code"},
            {DataType::INT32, "episode_of_id"},
            {DataType::INT32, "season_nr"},
            {DataType::INT32, "episode_nr"},
            {DataType::VARCHAR, "series_years"},
            {DataType::VARCHAR, "md5sum"}}                                    },
    {"movie_info",
     {{DataType::INT32, "id"},
            {DataType::INT32, "movie_id"},
            {DataType::INT32, "info_type_id"},
            {DataType::VARCHAR, "info"},
            {DataType::VARCHAR, "note"}}                                      },
    {"person_info",
     {{DataType::INT32, "id"},
            {DataType::INT32, "person_id"},
            {DataType::INT32, "info_type_id"},
            {DataType::VARCHAR, "info"},
            {DataType::VARCHAR, "note"}}                                      }
};

const Attribute* find_attribute(Table& table, const std::string& attr_name) {
    for (auto& attr: table.attributes()) {
        if (attr.name == attr_name) {
            return &attr;
        }
    }
    throw std::runtime_error(std::format("Cannot find attribute: {}", attr_name));
}

PlanNode* parse_node(const json& j, std::map<std::string, Table>& tables) {
    std::string type = j["Node Type"];

    if (type == "Nested Loop" || type == "Hash Join") {
        auto node = new PlanNode();
        if (type == "Nested Loop") {
            node->type = NodeType::NestedLoopJoin;
        } else if (type == "Hash Join") {
            node->type = NodeType::HashJoin;
        } else if (type == "Merge Join") {
            node->type = NodeType::SortMergeJoin;
        } else {
            throw std::runtime_error(std::format("No such join: {}", type));
        }
        // analzye sub trees recursively
        node->left  = parse_node(j["Left"], tables);
        node->right = parse_node(j["Right"], tables);

        // process joining conditions
        auto get_attr = [&tables](const json& attr_j) -> const Attribute* {
            std::string tbl  = attr_j["Table"];
            std::string attr = attr_j["Attr"];
            if (auto itr = tables.find(tbl); itr != tables.end()) {
                return find_attribute(itr->second, attr);
            } else {
                throw std::runtime_error(std::format("Cannot find table: {}", tbl));
            }
        };

        node->leftAttr  = get_attr(j["Left Attr"]);
        node->rightAttr = get_attr(j["Right Attr"]);

        // process build side
        if (type == "Hash Join") {
            node->build_left = j["Build Side"] == "Left";
        }

        return node;
    } else if (type == "Scan") {
        auto node  = new PlanNode();
        node->type = NodeType::Scan;

        std::string                rel_name = j["Relation Name"];
        std::string                csv_path = std::format("imdb/{}.csv", rel_name);
        std::shared_ptr<Statement> filter;

        if (j.contains("Filter") and not j["Filter"].is_null()) {
            filter = build_statement(j["Filter"].get<std::string>());
        }

        // create Table object
        auto table      = Table::from_csv(attributes_map[rel_name], csv_path, filter.get());
        auto [itr, _]   = tables.emplace(rel_name, std::move(table));
        node->baseTable = &itr->second;

        return node;
    }

    throw std::runtime_error("Unknown node type: " + type);
}

void delete_plan_node(PlanNode* node) {
    if (!node) {
        return;
    }
    delete_plan_node(node->left);
    delete_plan_node(node->right);
    delete node;
}

int main(int argc, char* argv[]) {
    // load plan json
    FILE* file       = fopen(std::format("tasks/{}.json", argv[1]).c_str(), "rb");
    json  query_plan = json::parse(file);
    std::map<std::string, Table> tables;

    // build Plan from json
    PlanNode* root = parse_node(query_plan, tables);

    // execute the plan and time
    auto start   = std::chrono::steady_clock::now();
    auto results = root->execute();
    auto end     = std::chrono::steady_clock::now();

    // print results
    results.print();
    std::println("{}, {}", results.number_rows(), results.number_cols());
    std::println("{}", std::chrono::duration_cast<std::chrono::milliseconds>(end - start));

    delete_plan_node(root);
    return 0;
}
