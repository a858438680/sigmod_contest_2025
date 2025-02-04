// #include <print>
// #include <ranges>

#include <csv_parser.h>
#include <spc_executor.h>

template <class Functor>
class TableParser: public CSVParser {
public:
    size_t               row_off_;
    std::vector<Data>    last_record_;
    std::span<Attribute> attributes_;
    Functor              add_record_fn_;

    template <class F>
    TableParser(std::span<Attribute> attributes,
        F&&                          functor_,
        char                         escape = '"',
        char sep                            = ',',
        bool has_trailing_comma             = false,
        bool has_header                     = false)
    : CSVParser(escape, sep, has_trailing_comma)
    , attributes_(attributes)
    , row_off_(has_header ? static_cast<size_t>(-1) : 0z)
    , add_record_fn_(static_cast<F&&>(functor_)) {}

    void on_field(size_t col_idx, size_t row_idx, const char* begin, size_t len) override {
        if (row_idx + this->row_off_ == static_cast<size_t>(-1)) {
            return;
        }
        if (len == 0z) {
            this->last_record_.emplace_back();
        } else {
            switch (this->attributes_[col_idx].type) {
            case DataType::INT32: {
                int32_t value;
                auto    result = std::from_chars(begin, begin + len, value);
                if (result.ec != std::errc()) {
                    throw std::runtime_error("parse integer error");
                }
                this->last_record_.emplace_back(value);
                break;
            }
            case DataType::INT64: {
                int64_t value;
                auto    result = std::from_chars(begin, begin + len, value);
                if (result.ec != std::errc()) {
                    throw std::runtime_error("parse integer error");
                }
                this->last_record_.emplace_back(value);
                break;
            }
            case DataType::FP64: {
                double value;
                auto   result = std::from_chars(begin, begin + len, value);
                if (result.ec != std::errc()) {
                    throw std::runtime_error("parse float error");
                }
                this->last_record_.emplace_back(value);
                break;
            }
            // case DataType::VARCHAR_255:
            case DataType::VARCHAR:     {
                this->last_record_.emplace_back(std::string{begin, len});
                break;
            }
            }
        }
        if (col_idx + 1 == this->attributes_.size()) {
            this->add_record_fn_(std::move(this->last_record_));
            this->last_record_.clear();
            this->last_record_.reserve(attributes_.size());
        }
    }
};

template <class F>
TableParser(std::span<Attribute> attributes,
    F&&                          functor_,
    char                         escape = '"',
    char sep                            = ',',
    bool has_trailing_comma             = false,
    bool has_header                     = false) -> TableParser<std::remove_cvref_t<F>>;

char buffer[1024 * 1024];

Table Table::from_csv(std::span<Attribute> attributes,
    const std::filesystem::path&           path,
    Statement*                             filter,
    bool                                   header) {
    std::vector<std::vector<Data>> filtered_table;
    FILE*                          fp = fopen(path.string().c_str(), "rb");
    auto add_record = [&filtered_table, attributes, filter](std::vector<Data>&& record) {
        if (not filter or filter->eval(attributes, record)) {
            filtered_table.emplace_back(std::move(record));
        }
    };
    TableParser parser(attributes, std::move(add_record), '\\', ',', false, header);
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
    Table table;
    table.set_attributes(attributes);
    table.data_ = std::move(filtered_table);
    return table;
}
