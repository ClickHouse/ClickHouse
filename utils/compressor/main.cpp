#include <iostream>

#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>


/// Выводит размеры разжатых и сжатых блоков для сжатого файла.
void stat(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
	while (!in.eof())
	{
		char header[QUICKLZ_HEADER_SIZE];

		in.ignore(16);	/// checksum
		in.readStrict(header, QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(header);
		if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
			throw DB::Exception("Too large size_compressed. Most likely corrupted data.", DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		size_t size_decompressed = qlz_size_decompressed(header);

		DB::writeText(size_decompressed, out);
		DB::writeChar('\t', out);
		DB::writeText(size_compressed, out);
		DB::writeChar('\n', out);

		in.ignore(size_compressed - QUICKLZ_HEADER_SIZE);
	}
}


int main(int argc, char ** argv)
{
	try
	{
		bool decompress = false;
		bool use_qlz = false;
		bool stat_mode = false;

		if (argc == 2)
		{
			decompress = 0 == strcmp(argv[1], "-d");
			use_qlz = 0 == strcmp(argv[1], "--qlz");
			stat_mode = 0 == strcmp(argv[1], "--stat");
		}
		
		if (argc > 2 || (argc == 2 && !decompress && !use_qlz && !stat_mode))
		{
			std::cerr << "Usage: " << argv[0] << " [-d|--qlz|--stat] < in > out" << std::endl;
			return 1;
		}

		DB::CompressionMethod::Enum method = use_qlz ? DB::CompressionMethod::QuickLZ : DB::CompressionMethod::LZ4;

		DB::ReadBufferFromFileDescriptor rb(STDIN_FILENO);
		DB::WriteBufferFromFileDescriptor wb(STDOUT_FILENO);

		if (stat_mode)
		{
			/// Вывести статистику для сжатого файла.
			stat(rb, wb);
		}
		else if (decompress)
		{
			/// Разжатие
			DB::CompressedReadBuffer from(rb);
			DB::copyData(from, wb);
		}
		else
		{
			/// Сжатие
			DB::CompressedWriteBuffer to(wb, method);
			DB::copyData(rb, to);
		}
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
