#include <iostream>

#include <boost/program_options.hpp>

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
	boost::program_options::options_description desc("Allowed options");
	desc.add_options()
		("help,h", "produce help message")
		("d,decompress", "decompress")
		("block-size,b", boost::program_options::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
		("hc", "use LZ4HC instead of LZ4")
		("qlz", "use QuickLZ (level 1) instead of LZ4")
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
		bool decompress = options.count("d");
		bool use_qlz = options.count("qlz");
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
