#include <list>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <boost/program_options.hpp>


int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message")
    ("address,a", boost::program_options::value<std::string>()->required(),
     "addresses of ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
    ("path,p", boost::program_options::value<std::string>()->required(), "path of replica queue to insert node (without trailing slash)")
    ("name,n", boost::program_options::value<std::string>()->required(), "name of part to download")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Insert log entry to replication queue to download part from any replica." << std::endl;
        std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    std::string path = options.at("path").as<std::string>();
    std::string name = options.at("name").as<std::string>();

    zkutil::ZooKeeper zookeeper(options.at("address").as<std::string>());

    DB::ReplicatedMergeTreeLogEntry entry;
    entry.type = DB::ReplicatedMergeTreeLogEntry::MERGE_PARTS;
    entry.parts_to_merge = {name};
    entry.new_part_name = name;

    zookeeper.create(path + "/queue-", entry.toString(), zkutil::CreateMode::PersistentSequential);
    return 0;
}
catch (const Poco::Exception & e)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
