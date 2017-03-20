#include <iostream>

#include <boost/program_options.hpp>

#include <DB/Common/Exception.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>


namespace DB
{
	namespace ErrorCodes
	{
		extern const int TOO_LARGE_SIZE_COMPRESSED;
	}
}


/// Выводит размеры разжатых и сжатых блоков для сжатого файла.
void stat(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
	while (!in.eof())
	{
		in.ignore(16);	/// checksum

		char header[COMPRESSED_BLOCK_HEADER_SIZE];
		in.readStrict(header, COMPRESSED_BLOCK_HEADER_SIZE);

		UInt32 size_compressed = unalignedLoad<UInt32>(&header[1]);

		if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
			throw DB::Exception("Too large size_compressed. Most likely corrupted data.", DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		UInt32 size_decompressed = unalignedLoad<UInt32>(&header[5]);

		DB::writeText(size_decompressed, out);
		DB::writeChar('\t', out);
		DB::writeText(size_compressed, out);
		DB::writeChar('\n', out);

		in.ignore(size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
	}
}


int main(int argc, char ** argv)
{
	boost::program_options::options_description desc("Allowed options");
	desc.add_options()
		("help,h", "produce help message")
		("decompress,d", "decompress")
		("block-size,b", boost::program_options::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
		("hc", "use LZ4HC instead of LZ4")
	#ifdef USE_QUICKLZ
		("qlz", "use QuickLZ (level 1) instead of LZ4")
	#endif
		("zstd", "use ZSTD instead of LZ4")
		("stat", "print block statistics of compressed data")
	;

	boost::program_options::variables_map options;
	boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

	if (options.count("help"))
	{
		std::cout << "Usage: " << argv[0] << " [options] < in > out" << std::endl;
		std::cout << desc << std::endl;
		return 1;
	}

	try
	{
		bool decompress = options.count("decompress");

	#ifdef USE_QUICKLZ
		bool use_qlz = options.count("qlz");
	#else
		bool use_qlz = false;
	#endif

		bool use_lz4hc = options.count("hc");
		bool use_zstd = options.count("zstd");
		bool stat_mode = options.count("stat");
		unsigned block_size = options["block-size"].as<unsigned>();

		DB::CompressionMethod method = DB::CompressionMethod::LZ4;

		if (use_qlz)
			method = DB::CompressionMethod::QuickLZ;
		else if (use_lz4hc)
			method = DB::CompressionMethod::LZ4HC;
		else if (use_zstd)
			method = DB::CompressionMethod::ZSTD;

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
			DB::CompressedWriteBuffer to(wb, method, block_size);
			DB::copyData(rb, to);
		}
	}
	catch (...)
	{
		std::cerr << DB::getCurrentExceptionMessage(true);
		return DB::getCurrentExceptionCode();
	}

    return 0;
}
