#include "ChunkBuffer.h"

namespace local_engine
{
void ChunkBuffer::add(DB::Chunk & columns, int start, int end)
{
    if (accumulated_columns.empty())
    {
        auto num_cols = columns.getNumColumns();
        accumulated_columns.reserve(num_cols);
        for (size_t i = 0; i < num_cols; i++)
        {
            accumulated_columns.emplace_back(columns.getColumns()[i]->cloneEmpty());
        }
    }

    for (size_t i = 0; i < columns.getNumColumns(); ++i)
        accumulated_columns[i]->insertRangeFrom(*columns.getColumns()[i], start, end - start);
}
size_t ChunkBuffer::size() const
{
    if (accumulated_columns.empty())
        return 0;
    return accumulated_columns.at(0)->size();
}
DB::Chunk ChunkBuffer::releaseColumns()
{
    auto rows = size();
    DB::Columns res(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    return DB::Chunk(res, rows);
}

}
