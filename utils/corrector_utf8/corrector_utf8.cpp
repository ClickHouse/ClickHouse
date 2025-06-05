#include <iostream>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferValidUTF8.h>
#include <IO/copyData.h>

int main(int, char **)
{
    using namespace DB;
    ReadBufferFromFileDescriptor rb(STDIN_FILENO);
    WriteBufferFromFileDescriptor wb(STDOUT_FILENO);
    WriteBufferValidUTF8 utf8_b(wb);
    copyData(rb, utf8_b);
    return 0;
}
