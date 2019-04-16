#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <boost/program_options.hpp>
#include <IO/ReadHelpers.h>

#include <unordered_map>
#include <cmath>

size_t getMaxBlockSizeForPartition(zkutil::ZooKeeper & zk,
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
                std::cerr << "Exception on: " << ex.displayText() << " will skip part: " << part << std::endl;
            }
        }
    }
    return max_block_num;
}

std::unordered_map<std::string, size_t> getAllTablesBlockPaths(zkutil::ZooKeeper & zk, const std::string & root)
{
    std::unordered_map<std::string, size_t> result;
    auto shards = zk.getChildren(root);
    for (const auto & shard : shards)
    {
        std::string shard_path = root + "/" + shard;
        auto tables = zk.getChildren(shard_path);
        for (auto table : tables)
        {
            std::cerr << "Searching for nodes in: " << table << std::endl;
            std::string table_path = shard_path + "/" + table;
            auto format_version = DB::ReplicatedMergeTreeTableMetadata::parse(zk.get(table_path + "/metadata")).data_format_version;
            std::string blocks_path = table_path + "/block_numbers";
            auto partitions = zk.getChildren(blocks_path);
            if (!partitions.empty())
            {
                for (auto partition : partitions)
                {
                    std::string part_path = blocks_path + "/" + partition;
                    size_t partition_max_block = getMaxBlockSizeForPartition(zk, table_path, partition, format_version);
                    std::cerr << "\tFound max block number: " << partition_max_block << " for part: " << partition << std::endl;
                    result.emplace(part_path, partition_max_block);
                }
            }
        }
    }
    return result;
}


void rotateNodes(zkutil::ZooKeeper & zk, const std::string & path, size_t max_block_num)
{
    Coordination::Requests requests;
    std::string block_prefix = path + "/block-";
    std::string current = zk.create(block_prefix, "", zkutil::CreateMode::EphemeralSequential);
    size_t current_block_num = DB::parse<UInt64>(current.c_str() + block_prefix.size(), current.size() - block_prefix.size());
    if (current_block_num >= max_block_num)
    {
        std::cerr << "Nothing to rotate, current block num: " << current_block_num << " max_block_num:" << max_block_num << std::endl;
        return;
    }

    size_t need_to_rotate = max_block_num - current_block_num;
    std::cerr << "Will rotate: " << need_to_rotate << " block numbers from " << current_block_num << " to " << max_block_num << std::endl;

    for (size_t i = 0; i < need_to_rotate; ++i)
    {
        if (requests.size() == 50)
        {
            std::cerr << "Rotating: " << i << " block numbers" << std::endl;
            zk.multi(requests);
            requests.clear();
        }
        requests.emplace_back(zkutil::makeCreateRequest(path + "/block-", "", zkutil::CreateMode::EphemeralSequential));
    }
    if (!requests.empty())
    {
        zk.multi(requests);
    }
}

int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message")
    ("address,a", boost::program_options::value<std::string>()->required(), "addresses of ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
    ("path,p", boost::program_options::value<std::string>()->required(), "path of replica queue to insert node (without trailing slash)");

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Util for /block_numbers node adjust with max block number in partition" << std::endl;
        std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    std::string global_path = options.at("path").as<std::string>();

    zkutil::ZooKeeper zookeeper(options.at("address").as<std::string>());

    auto all_path = getAllTablesBlockPaths(zookeeper, global_path);
    for (const auto & [path, max_block_num] : all_path)
    {
        std::cerr << "Rotating on: " << path << std::endl;
        rotateNodes(zookeeper, path, max_block_num);
    }
    return 0;
}
catch (const Poco::Exception & e)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
