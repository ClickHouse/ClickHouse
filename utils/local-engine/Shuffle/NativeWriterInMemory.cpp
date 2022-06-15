#include "NativeWriterInMemory.h"

using namespace DB;

namespace local_engine
{

NativeWriterInMemory::NativeWriterInMemory()
{
    write_buffer = std::make_unique<WriteBufferFromOwnString>();
}
void NativeWriterInMemory::write(Block & block)
{
    if (block.columns() == 0 || block.rows() == 0) return;
    if (!writer)
    {
        writer = std::make_unique<NativeWriter>(*write_buffer, 0, block.cloneEmpty());
    }
    writer->write(block);
}
std::string & NativeWriterInMemory::collect()
{
    return write_buffer->str();
}
}
