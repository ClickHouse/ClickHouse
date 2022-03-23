#pragma once
#include <IO/ReadBuffer.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Compression/CompressedReadBuffer.h>


using namespace DB;

namespace local_engine
{
class ShuffleReader
{
public:
    explicit ShuffleReader(std::unique_ptr<ReadBuffer> in_, bool compressed);
    Block read();
    ~ShuffleReader();
private:
    std::unique_ptr<ReadBuffer> in;
    std::unique_ptr<CompressedReadBuffer> compressed_in;
    std::unique_ptr<NativeBlockInputStream> input_stream;
};

}
