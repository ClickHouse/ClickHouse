#include <iostream>

#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/IO/copyData.h>

int main(int argc, char **argv)
{
	DB::ReadBufferFromFileDescriptor rb(STDIN_FILENO);
	DB::WriteBufferFromFileDescriptor wb(STDOUT_FILENO);
	DB::WriteBufferValidUTF8 utf8_b(wb);
	DB::copyData(rb, utf8_b);
	return 0;
}
