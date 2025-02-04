/*
 * Copyright 2025 Matthias Boehm, TU Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// API of the SIGMOD 2025 Programming Contest,
// See https://sigmod-contest-2025.github.io/index.html

#include <filesystem>
#include <span>

#include <cstddef>
#include <cstdint>

#include <statement.h>

constexpr int MAX_ATTR_NAME_LEN = 32;
constexpr int MAX_VARCHAR_LEN   = 255;
constexpr int PAGE_SIZE         = 8192;


// error codes for API functions
enum class ErrCode {
    SUCCESS,
    OUT_OF_MEMORY,
    FAILURE
};

// supported attribute data types
enum class DataType {
    INT32, // 4-byte integer
    INT64, // 8-byte integer
    FP64,  // 8-byte floating point
    // VARCHAR_255, // string of max length 255
    VARCHAR, // string of arbitary length
};

// attribute meta data
struct Attribute {
    DataType type;
    char     name[MAX_ATTR_NAME_LEN + 1];
};

// data page in PAX page layout
// Page header: pid, 2B #rows (n), 1B #attributes (m),
//   m 2B offsets to minipages, m 1B attribute sizes in byte
// Data: m minipages of consecutive value arrays, for the varchar
//   data type, there is a mini-header of n 1B value sizes in byte
// struct Page {
//     int32_t   pid;
//     std::byte data[PAGE_SIZE - 4];
// };

struct Table {
public:
    static Table from_csv(std::span<Attribute> attributes,
        const std::filesystem::path&           path,
        Statement*                             filter,
        bool                                   header = false);

private:
    std::vector<Attribute>         attributes_;
    std::vector<std::vector<Data>> data_;

    void set_attributes(std::span<Attribute> attributes) {
        this->attributes_.clear();
        for (auto& attr: attributes) {
            this->attributes_.push_back(attr);
        }
    }
};

// // table of schema, stats, and array of pages
// struct Table {
//     int64_t            nrows;
//     int32_t            ncols;
//     int64_t            npages;
//     Attribute*         attributes;
//     std::vector<Page*> pages;

//     static Table from_csv(std::span<Attribute> attributes,
//         const std::filesystem::path&           path,
//         Statement*                             filter,
//         bool                                   header = false);

//     Table()
//     : nrows(0)
//     , ncols(0)
//     , npages(0)
//     , attributes(nullptr)
//     , pages() {}

//     Table(const Table&)            = delete;
//     Table& operator=(const Table&) = delete;

//     Table(Table&& x) noexcept
//     : nrows(x.nrows)
//     , ncols(x.ncols)
//     , npages(std::exchange(x.npages, 0))
//     , attributes(std::exchange(x.attributes, nullptr))
//     , pages(std::move(x.pages)) {}

//     Table& operator=(Table&& rhs) noexcept {
//         if (&rhs != this) {
//             delete[] this->attributes;
//             for (auto i = 0z; i < this->npages; ++i) {
//                 delete[] this->pages[i];
//             }
//             this->nrows      = rhs.nrows;
//             this->ncols      = rhs.ncols;
//             this->npages     = std::exchange(rhs.npages, 0);
//             this->attributes = std::exchange(rhs.attributes, nullptr);
//             this->pages      = std::move(rhs.pages);
//         }
//         return *this;
//     }

//     ~Table() {
//         delete[] this->attributes;
//         for (auto i = 0z; i < this->npages; ++i) {
//             delete[] this->pages[i];
//         }
//     }

// private:
//     void set_attributes(std::span<Attribute> attributes) {
//         this->ncols      = static_cast<uint32_t>(attributes.size());
//         this->attributes = new Attribute[this->ncols];
//         for (auto i = 0u; i < this->ncols; ++i) {
//             this->attributes[i] = attributes[i];
//         }
//     }
// };

enum class NodeType {
    NestedLoopJoin,
    HashJoin,
    SortMergeJoin,
    Scan,
};

// query execution plan of equi joins
// inner nodes: left and right defined, base table undefined
// leaf nodes: base table defined, left and right undefined
struct PlanNode {
    NodeType   type;
    PlanNode*  left;
    PlanNode*  right;
    Attribute* leftAttr;
    Attribute* rightAttr;
    int64_t    estCard;
    Table*     baseTable;
};

/**
 * Execute the given query execution plan and produce the
 * output table in PAX page layout.
 *
 * @param plan the query execution plan, including pointers to the base tables
 * @param output the result of the query execution
 * @return ErrCode
 * SUCCESS if successfully executed the query execution plan.
 * OUT_OF_MEMORY if memory allocation errors occured during plan execution.
 * FAILURE if execution failed for some other reason.
 */
ErrCode execute(PlanNode* plan, Table** output);
