#include <iostream>

#include <boost/program_options.hpp>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>

/*
 * Dump the Write Ahead Log file, outputs:
 * Part 0, Version: 0, Action : ADD_PART, Name: 4_1_1_0, Block:
   a Int32 Int32(size = 2), b Int32 Int32(size = 2), c Int32 Int32(size = 2)
 */

static void dump(const std::string & bin_path)
{
    DB::ReadBufferFromFile in(bin_path);
    DB::NativeBlockInputStream block_in(in, 0);
    DB::Block block;

    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    for (size_t part_num = 0; !in.eof(); ++part_num)
    {
        UInt8 version;
        String part_name;
        DB::MergeTreeWriteAheadLog::ActionType action_type;

        DB::readIntBinary(version, in);
        DB::readIntBinary(action_type, in);
        DB::readStringBinary(part_name, in);
        block = block_in.read();

        out << "Part " << part_num << ", Version: " << version
            << ", Action : " << (action_type == DB::MergeTreeWriteAheadLog::ActionType::ADD_PART ? "ADD_PART" : "DROP_PART")
            << ", Name: " << part_name << ", Block:\n";
        out << block.dumpStructure() << "\n";
        out << "\n" << DB::flush;
    }
}


int main(int argc, char ** argv)
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message");

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") || argc != 2)
    {
        std::cout << "Usage: " << argv[0] << " wal.bin" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    try
    {
        dump(argv[1]);
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.message() << std::endl
                  << std::endl
                  << "Stack trace:" << std::endl
                  << e.getStackTraceString() << std::endl;
        throw;
    }

    return 0;
}
