#include "ShuffleReader.h"
#include <Shuffle/ShuffleSplitter.h>

namespace local_engine
{

local_engine::ShuffleReader::ShuffleReader(std::unique_ptr<ReadBuffer> in_, bool compressed):in(std::move(in_))
{
    if (compressed)
    {
         compressed_in = std::make_unique<CompressedReadBuffer>(*in);
         input_stream = std::make_unique<NativeBlockInputStream>(*compressed_in, 0);
    }
    else
    {
        input_stream = std::make_unique<NativeBlockInputStream>(*in, 0);
    }
}
Block local_engine::ShuffleReader::read()
{
    return input_stream->read();
}
ShuffleReader::~ShuffleReader()
{
    in.reset();
    compressed_in.reset();
    input_stream.reset();
}
}
