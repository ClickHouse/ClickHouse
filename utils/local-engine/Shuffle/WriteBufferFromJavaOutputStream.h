#pragma once
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <jni.h>

namespace local_engine
{
class WriteBufferFromJavaOutputStream : public DB::BufferWithOwnMemory<DB::WriteBuffer>
{
public:
    static jclass output_stream_class;
    static jmethodID output_stream_write;
    static jmethodID output_stream_flush;

    WriteBufferFromJavaOutputStream(JavaVM * vm, jobject output_stream, jbyteArray buffer);
    ~WriteBufferFromJavaOutputStream() override;

private:
    void nextImpl() override;

protected:
    void finalizeImpl() override;

private:
    JavaVM * vm;
    jobject output_stream;
    jbyteArray buffer;
    size_t buffer_size;
};
}



