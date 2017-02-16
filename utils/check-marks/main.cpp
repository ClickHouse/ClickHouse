#include <iostream>

#include <boost/program_options.hpp>

#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/Operators.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>


/** This program checks correctness of .mrk (marks) file for corresponding compressed .bin file.
  */


namespace DB
{
	namespace ErrorCodes
	{
		extern const int TOO_LARGE_SIZE_COMPRESSED;
	}
}


/// Read and check header of compressed block. Print size of decompressed and compressed data.
void stat(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
	in.ignore(16);	/// checksum

	char header[COMPRESSED_BLOCK_HEADER_SIZE];
	in.readStrict(header, COMPRESSED_BLOCK_HEADER_SIZE);

	UInt32 size_compressed = unalignedLoad<UInt32>(&header[1]);

	if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
		throw DB::Exception("Too large size_compressed. Most likely corrupted data.", DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

	UInt32 size_decompressed = unalignedLoad<UInt32>(&header[5]);

	out << size_decompressed << '\t' << size_compressed << '\n' << DB::flush;
}


int main(int argc, char ** argv)
{
	boost::program_options::options_description desc("Allowed options");
	desc.add_options()
		("help,h", "produce help message")
	;

	boost::program_options::variables_map options;
	boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

	if (options.count("help") || argc != 3)
	{
		std::cout << "Usage: " << argv[0] << " file.mrk file.bin" << std::endl;
		std::cout << desc << std::endl;
		return 1;
	}

	try
	{
		DB::ReadBufferFromFile mrk_in(argv[1]);
		DB::ReadBufferFromFile bin_in(argv[2], 4096);	/// Small buffer size just to check header of compressed block.

		DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);

		while (!mrk_in.eof())
		{
			UInt64 offset_in_compressed_file = 0;
			UInt64 offset_in_decompressed_block = 0;

			DB::readBinary(offset_in_compressed_file, mrk_in);
			DB::readBinary(offset_in_decompressed_block, mrk_in);

			bin_in.seek(offset_in_compressed_file);
			stat(bin_in, out);
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
