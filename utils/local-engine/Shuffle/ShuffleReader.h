#pragma once
#include <IO/ReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Compression/CompressedReadBuffer.h>
#include <jni.h>


namespace local_engine
{
class ReadBufferFromJavaInputStream;
class ShuffleReader
{
public:
    explicit ShuffleReader(std::unique_ptr<DB::ReadBuffer> in_, bool compressed);
    DB::Block* read();
    ~ShuffleReader();
    static thread_local JNIEnv * env;
    static jclass input_stream_class;
    static jmethodID input_stream_read;
    std::unique_ptr<DB::ReadBuffer> in;

private:
    std::unique_ptr<DB::CompressedReadBuffer> compressed_in;
    std::unique_ptr<DB::NativeReader> input_stream;
    DB::Block header;
};


class ReadBufferFromJavaInputStream : public DB::BufferWithOwnMemory<DB::ReadBuffer>
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
