#pragma once
#include <IO/ReadBuffer.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Compression/CompressedReadBuffer.h>
#include <jni.h>


using namespace DB;

namespace local_engine
{
class ReadBufferFromJavaInputStream;
class ShuffleReader
{
public:
    explicit ShuffleReader(std::unique_ptr<ReadBuffer> in_, bool compressed);
    Block* read();
    ~ShuffleReader();
    static thread_local JNIEnv * env;
    static jclass input_stream_class;
    static jmethodID input_stream_read;
    std::unique_ptr<ReadBuffer> in;

private:
    std::unique_ptr<CompressedReadBuffer> compressed_in;
    std::unique_ptr<NativeBlockInputStream> input_stream;
    std::unique_ptr<Block> cur_block;
    Block header;
};


class ReadBufferFromJavaInputStream : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit ReadBufferFromJavaInputStream(jobject input_stream);
    ~ReadBufferFromJavaInputStream() override;

private:
    jobject java_in;
    int readFromJava();
    bool nextImpl() override;


};

}
