#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <IO/ReadHelpers.h>

#include <unordered_map>
#include <cmath>


std::vector<std::string> getAllShards(zkutil::ZooKeeper & zk, const std::string & root)
{
    return zk.getChildren(root);
}


std::vector<std::string> removeNotExistingShards(zkutil::ZooKeeper & zk, const std::string & root, const std::vector<std::string> & shards)
{
    auto existing_shards = getAllShards(zk, root);
    std::vector<std::string> filtered_shards;
    filtered_shards.reserve(shards.size());
    for (const auto & shard : shards)
        if (std::find(existing_shards.begin(), existing_shards.end(), shard) == existing_shards.end())
            std::cerr << "Shard " << shard << " not found." << std::endl;
        else
            filtered_shards.emplace_back(shard);
    return filtered_shards;
}


std::vector<std::string> getAllTables(zkutil::ZooKeeper & zk, const std::string & root, const std::string & shard)
{
    return zk.getChildren(root + "/" + shard);
}


std::vector<std::string> removeNotExistingTables(zkutil::ZooKeeper & zk, const std::string & root, const std::string & shard, const std::vector<std::string> & tables)
{
    auto existing_tables = getAllTables(zk, root, shard);
    std::vector<std::string> filtered_tables;
    filtered_tables.reserve(tables.size());
    for (const auto & table : tables)
        if (std::find(existing_tables.begin(), existing_tables.end(), table) == existing_tables.end())
            std::cerr << "\tTable " << table << " not found on shard " << shard << "." << std::endl;
        else
            filtered_tables.emplace_back(table);
    return filtered_tables;
}


size_t getMaxBlockNumberForPartition(zkutil::ZooKeeper & zk,
    const std::string & replica_path,
    const std::string & partition_name,
    const DB::MergeTreeDataFormatVersion & format_version)
{
    auto replicas_path = replica_path + "/replicas";
    auto replica_hosts = zk.getChildren(replicas_path);
    size_t max_block_num = 0;
    for (const auto & replica_host : replica_hosts)
    {
        auto parts = zk.getChildren(replicas_path + "/" + replica_host + "/parts");
        for (const auto & part : parts)
        {
            try
            {
                auto info = DB::MergeTreePartInfo::fromPartName(part, format_version);
                if (info.partition_id == partition_name)
                    max_block_num = std::max<UInt64>(info.max_block, max_block_num);
            }
            catch (const DB::Exception & ex)
            {
                std::cerr << ex.displayText() << ", Part " << part << "skipped." << std::endl;
            }
        }
    }
    return max_block_num;
}


size_t getCurrentBlockNumberForPartition(zkutil::ZooKeeper & zk, const std::string & part_path)
{
    Coordination::Stat stat;
    zk.get(part_path, &stat);

    /// References:
    /// https://stackoverflow.com/a/10347910
    /// https://bowenli86.github.io/2016/07/07/distributed%20system/zookeeper/How-does-ZooKeeper-s-persistent-sequential-id-work/
    return (stat.cversion + stat.numChildren) / 2;
}


std::unordered_map<std::string, size_t> getPartitionsNeedAdjustingBlockNumbers(
    zkutil::ZooKeeper & zk, const std::string & root, const std::vector<std::string> & shards, const std::vector<std::string> & tables)
{
    std::unordered_map<std::string, size_t> result;

    std::vector<std::string> use_shards = shards.empty() ? getAllShards(zk, root) : removeNotExistingShards(zk, root, shards);

    for (const auto & shard : use_shards)
    {
        std::cout << "Shard: " << shard << std::endl;
        std::vector<std::string> use_tables = tables.empty() ? getAllTables(zk, root, shard) : removeNotExistingTables(zk, root, shard, tables);

        for (auto table : use_tables)
        {
            std::cout << "\tTable: " << table << std::endl;
            std::string table_path = root + "/" + shard + "/" + table;
            std::string blocks_path = table_path + "/block_numbers";

            std::vector<std::string> partitions;
            DB::MergeTreeDataFormatVersion format_version;
            try
            {
                format_version = DB::ReplicatedMergeTreeTableMetadata::parse(zk.get(table_path + "/metadata")).data_format_version;
                partitions = zk.getChildren(blocks_path);
            }
            catch (const DB::Exception & ex)
            {
                std::cerr << ex.displayText() << ", table " << table << " skipped." << std::endl;
                continue;
            }

            for (auto partition : partitions)
            {
                try
                {
                    std::string part_path = blocks_path + "/" + partition;
                    size_t partition_max_block = getMaxBlockNumberForPartition(zk, table_path, partition, format_version);
                    size_t current_block_number = getCurrentBlockNumberForPartition(zk, part_path);
                    if (current_block_number <= partition_max_block)
                    {
                        std::cout << "\t\tPartition: " << partition << ": current block_number: " << current_block_number
                                  << ", max block number: " << partition_max_block << ". Adjusting is required." << std::endl;
                        result.emplace(part_path, partition_max_block);
                    }
                }
                catch (const DB::Exception & ex)
                {
                    std::cerr << ex.displayText() << ", partition " << partition << " skipped." << std::endl;
                }
            }
        }
    }
    return result;
}


void setCurrentBlockNumber(zkutil::ZooKeeper & zk, const std::string & path, size_t new_current_block_number)
{
    std::string block_prefix = path + "/block-";
    Coordination::Requests requests;

    for (size_t current_block_number = getCurrentBlockNumberForPartition(zk, path);
         current_block_number < new_current_block_number;
         ++current_block_number)
    {
        if (requests.size() == 50)
        {
            zk.multi(requests);
            std::cout << path << ": " << requests.size() << " ephemeral sequential nodes inserted." << std::endl;
            requests.clear();
        }
        requests.emplace_back(zkutil::makeCreateRequest(path + "/block-", "", zkutil::CreateMode::EphemeralSequential));
    }

    if (!requests.empty())
    {
        std::cout << path << ": " << requests.size() << " ephemeral sequential nodes inserted." << std::endl;
        zk.multi(requests);
    }
}


int main(int argc, char ** argv)
try
{
    /// Parse the command line.
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "show help")
    ("zookeeper,z", po::value<std::string>(), "Addresses of ZooKeeper instances, comma-separated. Example: example01e.yandex.ru:2181")
    ("path,p", po::value<std::string>(), "[optional] Path of replica queue to insert node (without trailing slash). By default it's /clickhouse/tables")
    ("shard,s", po::value<std::string>(), "[optional] Shards to process, comma-separated. If not specified then the utility will process all the shards.")
    ("table,t", po::value<std::string>(), "[optional] Tables to process, comma-separated. If not specified then the utility will process all the tables.")
    ("dry-run", "[optional] Specify if you want this utility just to analyze block numbers without any changes.");

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    auto show_usage = [&]
    {
        std::cout << "Usage: " << std::endl;
        std::cout << "   " << argv[0] << " [options]" << std::endl;
        std::cout << desc << std::endl;
    };

    if (options.count("help") || (argc == 1))
    {
        std::cout << "This utility adjusts the /block_numbers zookeeper nodes to the correct block number in partition." << std::endl;
        std::cout << "It might be useful when incorrect block numbers stored in zookeeper don't allow you to insert data into a table or drop/detach a partition." << std::endl;
        show_usage();
       return 0;
    }

    if (!options.count("zookeeper"))
    {
        std::cerr << "Option --zookeeper should be set." << std::endl;
        show_usage();
        return 1;
    }

    std::string root = options.count("path") ? options.at("path").as<std::string>() : "/clickhouse/tables";

    std::vector<std::string> shards, tables;
    if (options.count("shard"))
        boost::split(shards, options.at("shard").as<std::string>(), boost::algorithm::is_any_of(","));
    if (options.count("table"))
        boost::split(tables, options.at("table").as<std::string>(), boost::algorithm::is_any_of(","));

    /// Check if the adjusting of the block numbers is required.
    std::cout << "Checking if adjusting of the block numbers is required:" << std::endl;
    zkutil::ZooKeeper zookeeper(options.at("zookeeper").as<std::string>());
    auto part_paths_with_max_block_numbers = getPartitionsNeedAdjustingBlockNumbers(zookeeper, root, shards, tables);

    if (part_paths_with_max_block_numbers.empty())
    {
        std::cout << "No adjusting required." << std::endl;
        return 0;
    }

    /// Adjust the block numbers.
    if (options.count("dry-run"))
        return 0;

    std::cout << std::endl << "Adjusting the block numbers:" << std::endl;
    for (const auto & [part_path, max_block_number] : part_paths_with_max_block_numbers)
        setCurrentBlockNumber(zookeeper, part_path, max_block_number + 1);

    return 0;
}
catch (const Poco::Exception & e)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
