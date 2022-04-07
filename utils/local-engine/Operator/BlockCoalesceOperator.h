#pragma once

#include <Shuffle/ShuffleSplitter.h>

namespace local_engine
{
class BlockCoalesceOperator
{
public:
    BlockCoalesceOperator(size_t buf_size_):buf_size(buf_size_){}
    void mergeBlock(DB::Block & block);
    bool isFull();
    DB::Block releaseBlock();

private:
    size_t buf_size;
    ColumnsBuffer block_buffer;
};
}


