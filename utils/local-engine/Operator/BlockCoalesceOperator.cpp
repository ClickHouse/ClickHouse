#include "BlockCoalesceOperator.h"
namespace local_engine
{
void BlockCoalesceOperator::mergeBlock(DB::Block & block)
{
    block_buffer.add(block, 0, block.rows());
}
bool BlockCoalesceOperator::isFull()
{
    return block_buffer.size() >= buf_size;
}
DB::Block BlockCoalesceOperator::releaseBlock()
{
    return block_buffer.releaseColumns();
}
}

