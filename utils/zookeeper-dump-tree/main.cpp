#include <list>
#include <iostream>
#include <boost/program_options.hpp>
#include "Common/ZooKeeper/Types.h"
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>


int main(int argc, char ** argv)
{
    try
    {
        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("help,h", "produce help message")
            ("address,a", boost::program_options::value<std::string>()->required(),
                "addresses of ZooKeeper instances, comma separated. Example: example01e.clickhouse.com:2181")
            ("path,p", boost::program_options::value<std::string>()->default_value("/"),
                "where to start")
            ("ctime,c", "print node ctime")
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

        bool dump_ctime = options.count("ctime");

        zkutil::ZooKeeperPtr zookeeper = std::make_shared<zkutil::ZooKeeper>(options.at("address").as<std::string>());

        std::string initial_path = options.at("path").as<std::string>();

        std::list<std::pair<std::vector<std::string>, std::future<Coordination::MultiResponse>>> multi_futures;
        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeListRequest(initial_path));
        requests.emplace_back(zkutil::makeGetRequest(initial_path));
        multi_futures.emplace_back(std::vector<std::string>{initial_path}, zookeeper->asyncMulti(requests));

        size_t num_reconnects = 0;
        constexpr size_t max_reconnects = 100;

        auto ensure_session = [&]
        {
            if (zookeeper->expired())
            {
                if (num_reconnects == max_reconnects)
                    return false;
                ++num_reconnects;
                zookeeper = zookeeper->startNewSession();
            }
            return true;
        };

        for (auto it = multi_futures.begin(); it != multi_futures.end(); ++it)
        {
            Coordination::MultiResponse response;

            try
            {
                response = it->second.get();
            }
            catch (const Coordination::Exception & e)
            {
                if (e.code == Coordination::Error::ZNONODE)
                {
                    continue;
                }
                else if (Coordination::isHardwareError(e.code))
                {
                    /// Reinitialize the session and move the node to the end of the queue for later retry.
                    if (!ensure_session())
                        throw;

                    auto paths = std::move(it->first);

                    Coordination::Requests new_requests;
                    for (const auto & path : paths)
                    {
                        new_requests.emplace_back(zkutil::makeListRequest(path));
                        new_requests.emplace_back(zkutil::makeGetRequest(path));
                    }
                    multi_futures.emplace_back(std::move(paths), zookeeper->asyncMulti(new_requests));
                    continue;
                }
                else
                    throw;
            }

            for (size_t i = 0; i < it->first.size(); ++i)
            {
                const auto & path = it->first[i];
                const auto & list_response = dynamic_cast<const Coordination::ListResponse &>(*response.responses[2 * i]);
                const auto & get_response = dynamic_cast<const Coordination::GetResponse &>(*response.responses[2 * i + 1]);
                std::cout << path << '\t' << list_response.stat.numChildren << '\t' << list_response.stat.dataLength;
                if (dump_ctime)
                    std::cout << '\t' << list_response.stat.ctime;
                std::cout << '\t' << get_response.data;
                std::cout << '\n';

                Coordination::Requests new_requests;
                std::vector<std::string> children;
                children.reserve(list_response.names.size());
                for (const auto & name : list_response.names)
                {
                    std::string child_path = path == "/" ? path + name : path + '/' + name;

                    ensure_session();
                    new_requests.emplace_back(zkutil::makeListRequest(child_path));
                    new_requests.emplace_back(zkutil::makeGetRequest(child_path));
                    children.push_back(std::move(child_path));
                }

                if (!children.empty())
                    multi_futures.emplace_back(std::move(children), zookeeper->asyncMulti(new_requests));
            }
        }

        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        return 1;
    }
}
