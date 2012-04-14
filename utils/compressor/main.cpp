#include <iostream>

#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/copyData.h>


int main(int argc, char ** argv)
{
	try
	{
		bool decompress = false;
		bool use_qlz = false;

		if (argc == 2)
		{
			decompress = 0 == strcmp(argv[1], "-d");
			use_qlz = 0 == strcmp(argv[1], "--qlz");
		}
		
		if (argc > 2 || (argc == 2 && !decompress && !use_qlz))
		{
			std::cerr << "Usage: " << argv[0] << " [-d|--qlz] < in > out" << std::endl;
			return 1;
		}

		DB::CompressionMethod::Enum method = use_qlz ? DB::CompressionMethod::QuickLZ : DB::CompressionMethod::LZ4;

		Poco::SharedPtr<DB::ReadBuffer> rb = new DB::ReadBufferFromFileDescriptor(STDIN_FILENO);
		Poco::SharedPtr<DB::WriteBuffer> wb = new DB::WriteBufferFromFileDescriptor(STDOUT_FILENO);
		Poco::SharedPtr<DB::ReadBuffer> from;
		Poco::SharedPtr<DB::WriteBuffer> to;

		if (!decompress)
		{
			/// Сжатие
			from = rb;
			to = new DB::CompressedWriteBuffer(*wb, method);
		}
		else
		{
			/// Разжатие
			from = new DB::CompressedReadBuffer(*rb);
			to = wb;
		}

		DB::copyData(*from, *to);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString()
			<< std::endl;
		throw;
	}

    return 0;
}
