// #include <print>
// #include <ranges>

#include <common.h>
#include <csv_parser.h>
#include <plan.h>
#include <table.h>

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
    , row_off_(has_header ? static_cast<size_t>(-1) : 0zu)
    , add_record_fn_(static_cast<F&&>(functor_)) {}

    void on_field(size_t col_idx, size_t row_idx, const char* begin, size_t len) override {
        if (row_idx + this->row_off_ == static_cast<size_t>(-1)) {
            return;
        }
        if (len == 0zu) {
            this->last_record_.emplace_back(std::monostate{});
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
            case DataType::VARCHAR: {
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
    File                           fp(path, "rb");
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

bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit      = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}

Table Table::from_columnar(const ColumnarTable& table) {
    namespace views = std::views;
    std::vector<std::vector<Data>> columns;
    columns.reserve(table.columns.size());
    std::vector<DataType> types;
    types.reserve(table.columns.size());
    for (const auto& [col_idx, column]: table.columns | views::enumerate) {
        types.emplace_back(column.type);
        std::vector<Data> new_column;
        new_column.reserve(table.num_rows);
        for (auto* page:
            column.pages | views::transform([](auto* page) { return page->data; })) {
            switch (column.type) {
            case DataType::INT32: {
                auto  num_rows   = *reinterpret_cast<uint16_t*>(page);
                auto* data_begin = reinterpret_cast<int32_t*>(page + 4);
                auto* bitmap =
                    reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                uint16_t data_idx = 0;
                for (uint16_t i = 0; i < num_rows; ++i) {
                    if (get_bitmap(bitmap, i)) {
                        auto value = data_begin[data_idx++];
                        new_column.emplace_back(value);
                    } else {
                        new_column.emplace_back(std::monostate{});
                    }
                }
                break;
            }
            case DataType::INT64: {
                auto  num_rows   = *reinterpret_cast<uint16_t*>(page);
                auto* data_begin = reinterpret_cast<int64_t*>(page + 8);
                auto* bitmap =
                    reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                uint16_t data_idx = 0;
                for (uint16_t i = 0; i < num_rows; ++i) {
                    if (get_bitmap(bitmap, i)) {
                        auto value = data_begin[data_idx++];
                        new_column.emplace_back(value);
                    } else {
                        new_column.emplace_back(std::monostate{});
                    }
                }
                break;
            }
            case DataType::FP64: {
                auto  num_rows   = *reinterpret_cast<uint16_t*>(page);
                auto* data_begin = reinterpret_cast<double*>(page + 8);
                auto* bitmap =
                    reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                uint16_t data_idx = 0;
                for (uint16_t i = 0; i < num_rows; ++i) {
                    if (get_bitmap(bitmap, i)) {
                        auto value = data_begin[data_idx++];
                        new_column.emplace_back(value);
                    } else {
                        new_column.emplace_back(std::monostate{});
                    }
                }
                break;
            }
            case DataType::VARCHAR: {
                auto  num_rows     = *reinterpret_cast<uint16_t*>(page);
                auto  num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
                auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
                auto* data_begin   = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
                auto* string_begin = data_begin;
                auto* bitmap =
                    reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                uint16_t data_idx = 0;
                for (uint16_t i = 0; i < num_rows; ++i) {
                    if (get_bitmap(bitmap, i)) {
                        auto        offset = offset_begin[data_idx++];
                        std::string value{string_begin, data_begin + offset};
                        string_begin = data_begin + offset;
                        new_column.emplace_back(std::move(value));
                    } else {
                        new_column.emplace_back(std::monostate{});
                    }
                }
                break;
            }
            }
        }
        columns.emplace_back(std::move(new_column));
    }
    std::vector<std::vector<Data>> results;
    results.reserve(table.num_rows);
    for (auto i = 0zu; i < table.num_rows; ++i) {
        std::vector<Data> record;
        record.reserve(table.columns.size());
        for (auto j = 0zu; j < table.columns.size(); ++j) {
            record.emplace_back(columns[j][i]);
        }
        results.emplace_back(std::move(record));
    }
    return {results, types};
}

void set_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx     = idx / 8;
    auto bit          = idx % 8;
    bitmap[byte_idx] |= (1u << bit);
}

void unset_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx     = idx / 8;
    auto bit          = idx % 8;
    bitmap[byte_idx] &= ~(1u << bit);
}

ColumnarTable Table::to_columnar() const {
    auto& table      = this->data_;
    auto& data_types = this->types_;
    namespace views  = std::views;
    ColumnarTable ret;
    ret.num_rows = table.size();
    for (auto [col_idx, data_type]: data_types | views::enumerate) {
        ret.columns.emplace_back(data_type);
        auto& column = ret.columns.back();
        switch (data_type) {
        case DataType::INT32: {
            uint16_t             num_rows = 0;
            std::vector<int32_t> data;
            std::vector<uint8_t> bitmap;
            data.reserve(2048);
            bitmap.reserve(256);
            auto save_page = [&column, &num_rows, &data, &bitmap]() {
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                memcpy(page + 4, data.data(), data.size() * 4);
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                bitmap.clear();
            };
            for (auto& record: table) {
                auto& value = record[col_idx];
                std::visit(
                    [&save_page, &column, &num_rows, &data, &bitmap](const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, int32_t>) {
                            if (4 + (data.size() + 1) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            set_bitmap(bitmap, num_rows);
                            data.emplace_back(value);
                            ++num_rows;
                        } else if constexpr (std::is_same_v<T, std::monostate>) {
                            if (4 + (data.size()) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            unset_bitmap(bitmap, num_rows);
                            ++num_rows;
                        }
                    },
                    value);
            }
            if (num_rows != 0) {
                save_page();
            }
            break;
        }
        case DataType::INT64: {
            uint16_t             num_rows = 0;
            std::vector<int64_t> data;
            std::vector<uint8_t> bitmap;
            data.reserve(1024);
            bitmap.reserve(128);
            auto save_page = [&column, &num_rows, &data, &bitmap]() {
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                memcpy(page + 8, data.data(), data.size() * 8);
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                bitmap.clear();
            };
            for (auto& record: table) {
                auto& value = record[col_idx];
                std::visit(
                    [&save_page, &column, &num_rows, &data, &bitmap](const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, int64_t>) {
                            if (8 + (data.size() + 1) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            set_bitmap(bitmap, num_rows);
                            data.emplace_back(value);
                            ++num_rows;
                        } else if constexpr (std::is_same_v<T, std::monostate>) {
                            if (8 + (data.size()) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            unset_bitmap(bitmap, num_rows);
                            ++num_rows;
                        }
                    },
                    value);
            }
            if (num_rows != 0) {
                save_page();
            }
            break;
        }
        case DataType::FP64: {
            uint16_t             num_rows = 0;
            std::vector<double>  data;
            std::vector<uint8_t> bitmap;
            data.reserve(1024);
            bitmap.reserve(128);
            auto save_page = [&column, &num_rows, &data, &bitmap]() {
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                memcpy(page + 8, data.data(), data.size() * 8);
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                bitmap.clear();
            };
            for (auto& record: table) {
                auto& value = record[col_idx];
                std::visit(
                    [&save_page, &column, &num_rows, &data, &bitmap](const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, double>) {
                            if (8 + (data.size() + 1) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            set_bitmap(bitmap, num_rows);
                            data.emplace_back(value);
                            ++num_rows;
                        } else if constexpr (std::is_same_v<T, std::monostate>) {
                            if (8 + (data.size()) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            unset_bitmap(bitmap, num_rows);
                            ++num_rows;
                        }
                    },
                    value);
            }
            if (num_rows != 0) {
                save_page();
            }
            break;
        }
        case DataType::VARCHAR: {
            uint16_t              num_rows = 0;
            std::vector<char>     data;
            std::vector<uint16_t> offsets;
            std::vector<uint8_t>  bitmap;
            data.reserve(8192);
            offsets.reserve(4096);
            bitmap.reserve(512);
            auto save_page = [&column, &num_rows, &data, &offsets, &bitmap]() {
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(offsets.size());
                memcpy(page + 4, offsets.data(), offsets.size() * 2);
                memcpy(page + 4 + offsets.size() * 2, data.data(), data.size());
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                offsets.clear();
                bitmap.clear();
            };
            for (auto& record: table) {
                auto& value = record[col_idx];
                std::visit(
                    [&save_page, &column, &num_rows, &data, &offsets, &bitmap](
                        const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, std::string>) {
                            if (value.size() > PAGE_SIZE - 7) {
                                throw std::runtime_error(std::format(
                                    "string longer than {} characters is not supported",
                                    PAGE_SIZE - 7));
                            }
                            if (4 + (offsets.size() + 1) * 2 + (data.size() + value.size())
                                    + (num_rows / 8 + 1)
                                > PAGE_SIZE) {
                                save_page();
                            }
                            set_bitmap(bitmap, num_rows);
                            data.insert(data.end(), value.begin(), value.end());
                            offsets.emplace_back(data.size());
                            ++num_rows;
                        } else if constexpr (std::is_same_v<T, std::monostate>) {
                            if (4 + offsets.size() * 2 + data.size() + (num_rows / 8 + 1)
                                > PAGE_SIZE) {
                                save_page();
                            }
                            unset_bitmap(bitmap, num_rows);
                            ++num_rows;
                        } else {
                            throw std::runtime_error("not string or null");
                        }
                    },
                    value);
            }
            if (num_rows != 0) {
                save_page();
            }
            break;
        }
        }
    }
    return ret;
}
