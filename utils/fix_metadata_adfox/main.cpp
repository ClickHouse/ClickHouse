#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <boost/program_options.hpp>

using namespace DB;

int main(int argc, char ** argv)
{
    /// Parse the command line.
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "show help")(
        "zookeeper,z", po::value<std::string>(), "Addresses of ZooKeeper instances, comma-separated. Example: example01e.yandex.ru:2181")(
        "path,p",
        po::value<std::string>(),
        "Path to node with metadata")(
        "dry-run", "[optional] Specify if you want this utility just to analyze block numbers without any changes.");


    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    zkutil::ZooKeeper zookeeper(options.at("zookeeper").as<std::string>());
    std::string path = options.at("path").as<std::string>();

    auto children = zookeeper.getChildren(path);
    size_t total_tables_updated = 0;
    for (const auto & child : children)
    {
        //std::cerr << "Child:" << child << std::endl;
        std::string child_path = path + "/" + child + "/replicas/sas-5fri2abjotnqnt4n.db.yandex.net/metadata";
        //std::cerr << "Child metadata path:" << child_path << std::endl;
        std::string shared_metadata_path = path + "/" + child + "/metadata";

        //std::cerr << "Child shared metadata path:" << shared_metadata_path << std::endl;
        std::string shared_metadata_str = zookeeper.get(shared_metadata_path);

        ReplicatedMergeTreeTableMetadata shared_metadata = ReplicatedMergeTreeTableMetadata::parse(shared_metadata_str);

        std::string metadata;
        if (!zookeeper.tryGet(child_path, metadata))
            continue;
        ReplicatedMergeTreeTableMetadata old_metadata = ReplicatedMergeTreeTableMetadata::parse(metadata);
        if (shared_metadata.index_granularity_bytes == 0 && old_metadata.index_granularity_bytes != 0)
        {
            total_tables_updated++;
            std::cerr << "Shared Metadata From ZK:" << shared_metadata_str << std::endl;
            std::cerr << "Metadata From ZK:" << metadata << std::endl;
            old_metadata.index_granularity_bytes = 0;

            std::cerr << "Metadata After Fix:" << old_metadata.toString() << std::endl;
            zookeeper.set(child_path, old_metadata.toString());
        }
    }

    std::cerr << "Total tables:" << total_tables_updated << std::endl;
    if (options.count("dry-run"))
        std::cerr << "Dry run, exiting" << std::endl;

    std::cerr << "Metadata updated\n";

    return 0;
}
