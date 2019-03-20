#include <iostream>

#include <boost/program_options.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Compression/CompressedReadBufferFromFile.h>


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
std::pair<UInt32, UInt32> stat(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
    if (in.eof())
        return {};

    in.ignore(16);    /// checksum

    char header[COMPRESSED_BLOCK_HEADER_SIZE];
    in.readStrict(header, COMPRESSED_BLOCK_HEADER_SIZE);

    UInt32 size_compressed = unalignedLoad<UInt32>(&header[1]);

    if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw DB::Exception("Too large size_compressed. Most likely corrupted data.", DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

    UInt32 size_decompressed = unalignedLoad<UInt32>(&header[5]);

    return {size_compressed, size_decompressed};
}


void checkCompressedHeaders(const std::string & mrk_path, const std::string & bin_path)
{
    DB::ReadBufferFromFile mrk_in(mrk_path);
    DB::ReadBufferFromFile bin_in(bin_path, 4096);    /// Small buffer size just to check header of compressed block.

    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    for (size_t mark_num = 0; !mrk_in.eof(); ++mark_num)
    {
        UInt64 offset_in_compressed_file = 0;
        UInt64 offset_in_decompressed_block = 0;

        DB::readBinary(offset_in_compressed_file, mrk_in);
        DB::readBinary(offset_in_decompressed_block, mrk_in);

        out << "Mark " << mark_num << ", points to " << offset_in_compressed_file << ", " << offset_in_decompressed_block << ". ";

        bin_in.seek(offset_in_compressed_file);
        auto sizes = stat(bin_in, out);

        out << "Block sizes: " << sizes.first << ", " << sizes.second << '\n' << DB::flush;
    }
}


void checkByCompressedReadBuffer(const std::string & mrk_path, const std::string & bin_path)
{
    DB::ReadBufferFromFile mrk_in(mrk_path);
    DB::CompressedReadBufferFromFile bin_in(bin_path, 0, 0);

    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    bool mrk2_format = boost::algorithm::ends_with(mrk_path, ".mrk2");

    for (size_t mark_num = 0; !mrk_in.eof(); ++mark_num)
    {
        UInt64 offset_in_compressed_file = 0;
        UInt64 offset_in_decompressed_block = 0;
        UInt64 index_granularity_rows = 0;

        DB::readBinary(offset_in_compressed_file, mrk_in);
        DB::readBinary(offset_in_decompressed_block, mrk_in);

        out << "Mark " << mark_num << ", points to " << offset_in_compressed_file << ", " << offset_in_decompressed_block;

        if (mrk2_format)
        {
            DB::readBinary(index_granularity_rows, mrk_in);

            out << ", has rows after " << index_granularity_rows;
        }

        out << ".\n" << DB::flush;

        bin_in.seek(offset_in_compressed_file, offset_in_decompressed_block);
    }
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
        /// checkCompressedHeaders(argv[1], argv[2]);
        checkByCompressedReadBuffer(argv[1], argv[2]);
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
