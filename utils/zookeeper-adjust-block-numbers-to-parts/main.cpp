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


Int64 getMaxBlockNumberForPartition(zkutil::ZooKeeper & zk,
    const std::string & replica_path,
    const std::string & partition_name,
    const DB::MergeTreeDataFormatVersion & format_version)
{
    auto replicas_path = replica_path + "/replicas";
    auto replica_hosts = zk.getChildren(replicas_path);
    Int64 max_block_num = 0;
    for (const auto & replica_host : replica_hosts)
    {
        auto parts = zk.getChildren(replicas_path + "/" + replica_host + "/parts");
        for (const auto & part : parts)
        {
            try
            {
                auto info = DB::MergeTreePartInfo::fromPartName(part, format_version);
                if (info.partition_id == partition_name)
                    max_block_num = std::max<Int64>(info.max_block, max_block_num);
            }
            catch (const DB::Exception & ex)
            {
                std::cerr << ex.displayText() << ", Part " << part << "skipped." << std::endl;
            }
        }
    }
    return max_block_num;
}


Int64 getCurrentBlockNumberForPartition(zkutil::ZooKeeper & zk, const std::string & part_path)
{
    Coordination::Stat stat;
    zk.get(part_path, &stat);

    /// References:
    /// https://stackoverflow.com/a/10347910
    /// https://bowenli86.github.io/2016/07/07/distributed%20system/zookeeper/How-does-ZooKeeper-s-persistent-sequential-id-work/
    return (stat.cversion + stat.numChildren) / 2;
}


std::unordered_map<std::string, Int64> getPartitionsNeedAdjustingBlockNumbers(
    zkutil::ZooKeeper & zk, const std::string & root, const std::vector<std::string> & shards, const std::vector<std::string> & tables)
{
    std::unordered_map<std::string, Int64> result;

    std::vector<std::string> use_shards = shards.empty() ? getAllShards(zk, root) : removeNotExistingShards(zk, root, shards);

    for (const auto & shard : use_shards)
    {
        std::cout << "Shard: " << shard << std::endl;
        std::vector<std::string> use_tables = tables.empty() ? getAllTables(zk, root, shard) : removeNotExistingTables(zk, root, shard, tables);

        for (const auto & table : use_tables)
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

            for (const auto & partition : partitions)
            {
                try
                {
                    std::string part_path = blocks_path + "/" + partition;
                    Int64 partition_max_block = getMaxBlockNumberForPartition(zk, table_path, partition, format_version);
                    Int64 current_block_number = getCurrentBlockNumberForPartition(zk, part_path);
                    if (current_block_number < partition_max_block + 1)
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


void setCurrentBlockNumber(zkutil::ZooKeeper & zk, const std::string & path, Int64 new_current_block_number)
{
    Int64 current_block_number = getCurrentBlockNumberForPartition(zk, path);

    auto create_ephemeral_nodes = [&](size_t count)
    {
        std::string block_prefix = path + "/block-";
        Coordination::Requests requests;
        requests.reserve(count);
        for (size_t i = 0; i != count; ++i)
            requests.emplace_back(zkutil::makeCreateRequest(block_prefix, "", zkutil::CreateMode::EphemeralSequential));
        auto responses = zk.multi(requests);

        std::vector<std::string> paths_created;
        paths_created.reserve(responses.size());
        for (const auto & response : responses)
        {
            const auto * create_response = dynamic_cast<Coordination::CreateResponse*>(response.get());
            if (!create_response)
            {
                std::cerr << "\tCould not create ephemeral node " << block_prefix << std::endl;
                return false;
            }
            paths_created.emplace_back(create_response->path_created);
        }

        std::sort(paths_created.begin(), paths_created.end());
        for (const auto & path_created : paths_created)
        {
            Int64 number = DB::parse<Int64>(path_created.c_str() + block_prefix.size(), path_created.size() - block_prefix.size());
            if (number != current_block_number)
            {
                char suffix[11] = "";
                size_t size = sprintf(suffix, "%010lld", current_block_number);
                std::string expected_path = block_prefix + std::string(suffix, size);
                std::cerr << "\t" << path_created << ": Ephemeral node has been created with an unexpected path (expected something like "
                          << expected_path << ")." << std::endl;
                return false;
            }
            std::cout << "\t" << path_created << std::endl;
            ++current_block_number;
        }

        return true;
    };

    if (current_block_number >= new_current_block_number)
        return;

    std::cout << "Creating ephemeral sequential nodes:" << std::endl;
    create_ephemeral_nodes(1); /// Firstly try to create just a single node.

    /// Create other nodes in batches of 50 nodes.
    while (current_block_number + 50 <= new_current_block_number) // NOLINT: clang-tidy thinks that the loop is infinite
        create_ephemeral_nodes(50);

    create_ephemeral_nodes(new_current_block_number - current_block_number);
}


int main(int argc, char ** argv)
try
{
    /// Parse the command line.
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "show help")
    ("zookeeper,z", po::value<std::string>(), "Addresses of ZooKeeper instances, comma-separated. Example: example01e.clickhouse.com:2181")
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

    std::cout << "Required adjusting of " << part_paths_with_max_block_numbers.size() << " block numbers." << std::endl;

    /// Adjust the block numbers.
    if (options.count("dry-run"))
    {
        std::cout << "This is a dry-run, exiting." << std::endl;
        return 0;
    }

    std::cout << std::endl << "Adjusting the block numbers:" << std::endl;
    for (const auto & [part_path, max_block_number] : part_paths_with_max_block_numbers)
        setCurrentBlockNumber(zookeeper, part_path, max_block_number + 1);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
