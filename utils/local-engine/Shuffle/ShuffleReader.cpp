#include "ShuffleReader.h"
#include <Common/DebugUtils.h>
#include <Shuffle/ShuffleSplitter.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

using namespace DB;

namespace local_engine
{

local_engine::ShuffleReader::ShuffleReader(std::unique_ptr<ReadBuffer> in_, bool compressed):in(std::move(in_))
{
    if (compressed)
    {
         compressed_in = std::make_unique<CompressedReadBuffer>(*in);
         input_stream = std::make_unique<NativeReader>(*compressed_in, 0);
    }
    else
    {
        input_stream = std::make_unique<NativeReader>(*in, 0);
    }
}
Block* local_engine::ShuffleReader::read()
{
    Block *cur_block = new Block(input_stream->read());
    if (header.columns() == 0)
        header = cur_block->cloneEmpty();
    if (cur_block->columns() == 0)
    {
        delete cur_block;
        cur_block = new Block(header.cloneEmpty());
    }
    return cur_block;
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
    if (buf == nullptr)
    {
        buf = static_cast<jbyteArray>(ShuffleReader::env->NewGlobalRef(ShuffleReader::env->NewByteArray(4096)));
    }
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
    if (buf != nullptr)
    {
        ShuffleReader::env->DeleteGlobalRef(buf);
    }
}

}
