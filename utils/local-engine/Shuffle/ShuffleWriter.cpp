#include "ShuffleWriter.h"

using namespace DB;

namespace local_engine
{

ShuffleWriter::ShuffleWriter(JavaVM * vm, jobject output_stream, jbyteArray buffer)
{
    write_buffer = std::make_unique<WriteBufferFromJavaOutputStream>(vm, output_stream, buffer);
}
void ShuffleWriter::write(const Block & block)
{
    if (!native_writer)
    {
        native_writer = std::make_unique<NativeWriter>(*write_buffer, 0, block.cloneEmpty());
    }
    native_writer->write(block);
}
void ShuffleWriter::flush()
{
    if (native_writer)
    {
        native_writer->flush();
    }
}
ShuffleWriter::~ShuffleWriter()
{
    if (native_writer)
    {
        native_writer->flush();
        write_buffer->finalize();
    }
}
}
