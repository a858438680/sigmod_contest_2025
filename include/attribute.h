#pragma once

#include <string>

enum class DataType {
    INT32,       // 4-byte integer
    INT64,       // 8-byte integer
    FP64,        // 8-byte floating point
    VARCHAR,     // string of arbitary length
};

struct Attribute {
    DataType    type;
    std::string name;
};