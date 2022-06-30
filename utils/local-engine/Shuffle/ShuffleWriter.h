#pragma once
#include <Shuffle/WriteBufferFromJavaOutputStream.h>
#include <Formats/NativeWriter.h>

namespace local_engine
{
class ShuffleWriter
{
public:
    ShuffleWriter(JavaVM * vm, jobject output_stream, jbyteArray buffer);
    virtual ~ShuffleWriter();
    void write(const DB::Block & block);
    void flush();
private:
    std::unique_ptr<WriteBufferFromJavaOutputStream> write_buffer;
    std::unique_ptr<DB::NativeWriter> native_writer;
};
}



