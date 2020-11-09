#include <iostream>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferValidUTF8.h>
#include <IO/copyData.h>

int main(int argc, char **argv)
{
    DB::ReadBufferFromFileDescriptor rb(STDIN_FILENO);
    DB::WriteBufferFromFileDescriptor wb(STDOUT_FILENO);
    DB::WriteBufferValidUTF8 utf8_b(wb);
    DB::copyData(rb, utf8_b);
    return 0;
}
