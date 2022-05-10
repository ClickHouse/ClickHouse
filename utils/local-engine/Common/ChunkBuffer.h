#pragma once
#include <Processors/Chunk.h>

namespace local_engine
{
class ChunkBuffer
{
public:
    void add(DB::Chunk & columns, int start, int end);
    size_t size() const;
    DB::Chunk releaseColumns();

private:
    DB::MutableColumns accumulated_columns;
};

}
