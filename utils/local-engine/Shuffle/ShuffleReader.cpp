#include "ShuffleReader.h"
#include <Shuffle/ShuffleSplitter.h>
#include <Common/Exception.h>



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
Block* local_engine::ShuffleReader::read()
{
    cur_block.reset();
    cur_block = std::make_unique<Block>(input_stream->read());
    if (header.columns() == 0) header = cur_block->cloneEmpty();
    if (cur_block->columns() == 0) cur_block = std::make_unique<Block>(header.cloneEmpty());
    return cur_block.get();
}
ShuffleReader::~ShuffleReader()
{
    in.reset();
    compressed_in.reset();
    input_stream.reset();
}

thread_local JNIEnv * ShuffleReader::env = nullptr;
jclass ShuffleReader::input_stream_class = nullptr;
jmethodID ShuffleReader::input_stream_read = nullptr;

bool ReadBufferFromJavaInputStream::nextImpl()
{
    int count = readFromJava();
    if (count > 0)
    {
        working_buffer.resize(count);
    }
    return count > 0;
}
int ReadBufferFromJavaInputStream::readFromJava()
{
    assert(ShuffleReader::env != nullptr);
    jbyteArray buf = ShuffleReader::env->NewByteArray(internal_buffer.size());
    jint count = ShuffleReader::env->CallIntMethod(java_in, ShuffleReader::input_stream_read, buf);
    if (count > 0)
    {
        ShuffleReader::env->GetByteArrayRegion(buf, 0, count, reinterpret_cast<jbyte *>(internal_buffer.begin()));
    }
    return count;
}
ReadBufferFromJavaInputStream::ReadBufferFromJavaInputStream(jobject input_stream)
    : java_in(input_stream)
{

}
ReadBufferFromJavaInputStream::~ReadBufferFromJavaInputStream()
{
    assert(ShuffleReader::env != nullptr);
    ShuffleReader::env->DeleteGlobalRef(java_in);
}

}
