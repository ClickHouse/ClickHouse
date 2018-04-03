#include <list>
#include <iostream>
#include <boost/program_options.hpp>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>


int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("address,a", boost::program_options::value<std::string>()->required(),
            "addresses of ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
        ("path,p", boost::program_options::value<std::string>()->default_value("/"),
            "where to start")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Dump paths of all nodes in ZooKeeper." << std::endl;
        std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    zkutil::ZooKeeper zookeeper(options.at("address").as<std::string>());

    std::string initial_path = options.at("path").as<std::string>();

    std::list<std::pair<std::string, std::future<zkutil::ListResponse>>> list_futures;
    list_futures.emplace_back(initial_path, zookeeper.asyncGetChildren(initial_path));

    for (auto it = list_futures.begin(); it != list_futures.end(); ++it)
    {
        zkutil::ListResponse response;
        try
        {
            response = it->second.get();
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code == ZooKeeperImpl::ZooKeeper::ZNONODE)
                continue;
            throw;
        }

        std::cout << it->first << '\t' << response.stat.numChildren << '\t' << response.stat.dataLength << '\n';

        for (const auto & name : response.names)
        {
            std::string child_path = it->first == "/" ? it->first + name : it->first + '/' + name;
            list_futures.emplace_back(child_path, zookeeper.asyncGetChildren(child_path));
        }
    }
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
