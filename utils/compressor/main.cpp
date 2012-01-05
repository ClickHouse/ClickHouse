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
		bool use_lz4 = false;

		if (argc == 2)
		{
			decompress = 0 == strcmp(argv[1], "-d");
			use_lz4 = 0 == strcmp(argv[1], "--lz4");
		}
		
		if (argc > 2 || (argc == 2 && !decompress && !use_lz4))
		{
			std::cerr << "Usage: " << argv[0] << " [-d|--lz4] < in > out" << std::endl;
			return 1;
		}

		DB::CompressionMethod::Enum method = use_lz4 ? DB::CompressionMethod::LZ4 : DB::CompressionMethod::QuickLZ;

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
