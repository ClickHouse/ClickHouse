#pragma once

#include <Core/Block.h>



namespace local_engine
{
class BlockIterator
{

protected:
    void checkNextValid();
    // make current block available
    void produce();
    // consume current block
    void consume();
    bool isConsumed() const;
    DB::Block & currentBlock();
    void setCurrentBlock(DB::Block & block);

private:
    DB::Block cached_block;
    bool consumed = true;
};
}

