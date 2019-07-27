#include "block.h"

#include <stdexcept>

namespace clickhouse {

Block::Iterator::Iterator(const Block& block)
    : block_(block)
    , idx_(0)
{
}

const std::string& Block::Iterator::Name() const {
    return block_.columns_[idx_].name;
}

TypeRef Block::Iterator::Type() const {
    return block_.columns_[idx_].column->Type();
}

ColumnRef Block::Iterator::Column() const {
    return block_.columns_[idx_].column;
}

void Block::Iterator::Next() {
    ++idx_;
}

bool Block::Iterator::IsValid() const {
    return idx_ < block_.columns_.size();
}


Block::Block()
    //: rows_(0)
{
}

/*Block::Block(size_t cols, size_t rows)
    //: rows_(rows)
{
    columns_.reserve(cols);
}*/

Block::~Block() = default;

void Block::AppendColumn(const std::string& name, const ColumnRef& col) {
    /*if (columns_.empty()) {
        rows_ = col->Size();
    } else if (col->Size() != rows_) {
        throw std::runtime_error("all columns in block must have same count of rows. Name: ["+name+"], rows: ["+std::to_string(rows_)+"], columns: [" + std::to_string(col->Size())+"]");
    }*/

    columns_.push_back(ColumnItem{name, col});
}

/// Count of columns in the block.
size_t Block::GetColumnCount() const {
    return columns_.size();
}

const BlockInfo& Block::Info() const {
    return info_;
}

/// Count of rows in the block.
size_t Block::GetRowCount() const {
    size_t rows = columns_.size() ? columns_[0].column->Size() : 0;
    for(size_t i=1; i<columns_.size(); i++) {
        ColumnRef col = columns_[i].column;
        if(col->Size()!=rows) {
            throw std::runtime_error("all columns in block must have same count of rows. Name: ["+columns_[i].name+"], rows: ["+std::to_string(rows)+"], columns: [" + std::to_string(col->Size())+"]");
        }
    }
    return rows;
}

ColumnRef Block::operator [] (size_t idx) const {
    if (idx < columns_.size()) {
        return columns_[idx].column;
    }

    throw std::out_of_range("column index is out of range. Index: ["+std::to_string(idx)+"], columns: [" + std::to_string(columns_.size())+"]");
}

}
