#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/localBackup.h>
#include <Common/Exception.h>

#include <boost/program_options.hpp>
#include <Poco/Path.h>
#include <Poco/File.h>

#include <iostream>
#include <Disks/DiskLocal.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int BAD_DATA_PART_NAME;
    extern const int NO_FILE_IN_DATA_PART;
}

void run(String part_path, String date_column, String dest_path)
{
    std::shared_ptr<IDisk> disk = std::make_shared<DiskLocal>("local", "/", 0);
    auto old_part_path = Poco::Path::forDirectory(part_path);
    const String & old_part_name = old_part_path.directory(old_part_path.depth() - 1);
    String old_part_path_str = old_part_path.toString();

    auto part_info = MergeTreePartInfo::fromPartName(old_part_name, MergeTreeDataFormatVersion(0));
    String new_part_name = part_info.getPartName();

    auto new_part_path = Poco::Path::forDirectory(dest_path);
    new_part_path.pushDirectory(new_part_name);
    if (Poco::File(new_part_path).exists())
        throw Exception("Destination part directory `" + new_part_path.toString() + "` already exists",
            ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    DayNum min_date;
    DayNum max_date;
    MergeTreePartInfo::parseMinMaxDatesFromPartName(old_part_name, min_date, max_date);

    const auto & time_zone = DateLUT::instance();
    UInt32 yyyymm = time_zone.toNumYYYYMM(min_date);
    if (yyyymm != time_zone.toNumYYYYMM(max_date))
        throw Exception("Part " + old_part_name + " spans different months",
            ErrorCodes::BAD_DATA_PART_NAME);

    ReadBufferFromFile checksums_in(old_part_path_str + "checksums.txt", 4096);
    MergeTreeDataPartChecksums checksums;
    checksums.read(checksums_in);

    auto date_col_checksum_it = checksums.files.find(date_column + ".bin");
    if (date_col_checksum_it == checksums.files.end())
        throw Exception("Couldn't find checksum for the date column .bin file `" + date_column + ".bin`",
            ErrorCodes::NO_FILE_IN_DATA_PART);

    UInt64 rows = date_col_checksum_it->second.uncompressed_size / DataTypeDate().getSizeOfValueInMemory();

    auto new_tmp_part_path = Poco::Path::forDirectory(dest_path);
    new_tmp_part_path.pushDirectory("tmp_convert_" + new_part_name);
    String new_tmp_part_path_str = new_tmp_part_path.toString();
    try
    {
        Poco::File(new_tmp_part_path).remove(/* recursive = */ true);
    }
    catch (const Poco::FileNotFoundException &)
    {
        /// If the file is already deleted, do nothing.
    }
    localBackup(disk, old_part_path.toString(), new_tmp_part_path.toString(), {});

    WriteBufferFromFile count_out(new_tmp_part_path_str + "count.txt", 4096);
    HashingWriteBuffer count_out_hashing(count_out);
    writeIntText(rows, count_out_hashing);
    count_out_hashing.next();
    checksums.files["count.txt"].file_size = count_out_hashing.count();
    checksums.files["count.txt"].file_hash = count_out_hashing.getHash();

    IMergeTreeDataPart::MinMaxIndex minmax_idx(min_date, max_date);
    Names minmax_idx_columns = {date_column};
    DataTypes minmax_idx_column_types = {std::make_shared<DataTypeDate>()};
    minmax_idx.store(minmax_idx_columns, minmax_idx_column_types, disk, new_tmp_part_path_str, checksums);

    Block partition_key_sample{{nullptr, std::make_shared<DataTypeUInt32>(), makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(date_column))->getColumnName()}};

    MergeTreePartition partition(yyyymm);
    partition.store(partition_key_sample, disk, new_tmp_part_path_str, checksums);
    String partition_id = partition.getID(partition_key_sample);

    Poco::File(new_tmp_part_path_str + "checksums.txt").setWriteable();
    WriteBufferFromFile checksums_out(new_tmp_part_path_str + "checksums.txt", 4096);
    checksums.write(checksums_out);
    checksums_in.close();
    checksums_out.close();

    Poco::File(new_tmp_part_path).renameTo(new_part_path.toString());
}

}

int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("part", boost::program_options::value<std::string>()->required(),
            "part directory to convert")
        ("date-column", boost::program_options::value<std::string>()->required(),
            "name of the date column")
        ("to", boost::program_options::value<std::string>()->required(),
            "destination directory")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") || options.size() < 3)
    {
        std::cout
            << "Convert a MergeTree part from the old-style month-partitioned table "
            << "(e.g. 20140317_20140323_2_2_0) to the format suitable for ATTACH'ing to a custom-partitioned "
            << "table (201403_2_2_0)." << std::endl << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    auto part_path = options.at("part").as<DB::String>();
    auto date_column = options.at("date-column").as<DB::String>();
    auto dest_path = options.at("to").as<DB::String>();

    DB::run(part_path, date_column, dest_path);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
