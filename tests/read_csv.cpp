#include <print>
#include <string_view>

#include <csv_parser.h>

class MyParser: public CSVParser {
public:
    using CSVParser::CSVParser;
    size_t                   row_idx_ = 0;
    std::vector<std::string> last_record_;

    void on_field(size_t col_idx, size_t row_idx, const char* begin, size_t len) override {
        if (row_idx == 0) {
            std::print("{},", std::string_view{begin, len});
        }
        if (row_idx != this->row_idx_) {
            last_record_.clear();
        }
        this->last_record_.emplace_back(std::string_view{begin, len});
        this->row_idx_ = row_idx;
    }
};

char buffer[1024 * 1024];

void test_csv(char* path) {
    FILE*    fp = fopen(path, "rb");
    MyParser parser('\\');
    while (true) {
        auto bytes_read = fread(buffer, 1, sizeof(buffer), fp);
        if (bytes_read != 0) {
            auto err = parser.execute(buffer, bytes_read);
            if (err != CSVParser::Ok) {
                throw std::runtime_error("CSV parse error");
            }
        } else {
            break;
        }
    }
    auto err = parser.finish();
    if (err != CSVParser::Ok) {
        throw std::runtime_error("CSV parse error");
    }
    std::println("");
    for (auto& field: parser.last_record_) {
        std::print("{},", field);
    }
    std::println("");
}

int main(int argc, char* argv[]) {
    std::println("read csv: {}", argv[1]);
    test_csv(argv[1]);
}
