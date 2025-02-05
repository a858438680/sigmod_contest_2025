#include <memory>
#include <nlohmann/json.hpp>
#include <unordered_map>

#include <spc_executor.h>

using json = nlohmann::json;

// 属性查找表
std::unordered_map<std::string, std::vector<Attribute>> attributes_map = {
    {"company_type",    {{DataType::INT32, "id"}, {DataType::VARCHAR, "kind"}}},
    {"info_type",       {{DataType::INT32, "id"}, {DataType::VARCHAR, "info"}}},
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
            {DataType::VARCHAR, "md5sum"}}                                    }
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
        // 递归解析子树
        node->left  = parse_node(j["Left"], tables);
        node->right = parse_node(j["Right"], tables);

        // 处理连接属性
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

        // 处理Hash Join属性
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

        if (j.contains("Filter")) {
            filter = build_statement(j["Filter"].get<std::string>());
        }

        // 创建Table对象
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
    delete_plan_node(node->left);  // 先删除左子树
    delete_plan_node(node->right); // 再删除右子树
    delete node;                   // 最后删除当前节点
}

int main() {
    // 加载JSON
    FILE*                        file       = fopen("tasks/1a.json", "rb");
    json                         query_plan = json::parse(file);
    std::map<std::string, Table> tables;

    // 构建执行计划
    PlanNode* root = parse_node(query_plan, tables);

    // 执行查询
    auto start   = std::chrono::steady_clock::now();
    auto results = root->execute();
    auto end     = std::chrono::steady_clock::now();

    results.print();
    std::println("{}", std::chrono::duration_cast<std::chrono::milliseconds>(end - start));

    delete_plan_node(root);
    return 0;
}
