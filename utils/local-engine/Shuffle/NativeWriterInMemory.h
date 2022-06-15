#pragma once
#include <Formats/NativeWriter.h>
#include <IO/WriteBufferFromString.h>

namespace local_engine
{
class NativeWriterInMemory
{
public:
    NativeWriterInMemory();
    void write(DB::Block & block);
    std::string & collect();

private:
    std::unique_ptr<DB::WriteBufferFromOwnString> write_buffer;
    //lazy init
    std::unique_ptr<DB::NativeWriter> writer;
};
}




