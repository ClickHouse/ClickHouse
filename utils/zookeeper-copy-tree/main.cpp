#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/Exception.h>

#include <boost/program_options.hpp>

#include <iostream>

namespace DB
{
namespace ErrorCodes
{

extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;

}
}

int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("from", boost::program_options::value<std::string>()->required(),
            "addresses of source ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
        ("from-path", boost::program_options::value<std::string>()->required(),
            "where to copy from")
        ("to", boost::program_options::value<std::string>()->required(),
            "addresses of destination ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
        ("to-path", boost::program_options::value<std::string>()->required(),
            "where to copy to")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Copy a ZooKeeper tree to another cluster." << std::endl;
        std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
        std::cout << "WARNING: it is almost useless as it is impossible to corretly copy sequential nodes" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    zkutil::ZooKeeper from_zookeeper(options.at("from").as<std::string>());
    zkutil::ZooKeeper to_zookeeper(options.at("to").as<std::string>());

    std::string from_path = options.at("from-path").as<std::string>();
    std::string to_path = options.at("to-path").as<std::string>();

    if (to_zookeeper.exists(to_path))
        throw DB::Exception("Destination path: " + to_path + " already exists, aborting.",
            DB::ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER);

    struct Node
    {
        Node(
            std::string path_,
            std::future<Coordination::GetResponse> get_future_,
            std::future<Coordination::ListResponse> children_future_,
            Node * parent_)
            : path(std::move(path_))
            , get_future(std::move(get_future_))
            , children_future(std::move(children_future_))
            , parent(parent_)
        {
        }

        std::string path;
        std::future<Coordination::GetResponse> get_future;
        std::future<Coordination::ListResponse> children_future;

        Node * parent = nullptr;
        std::future<Coordination::CreateResponse> create_future;
        bool created = false;
        bool deleted = false;
        bool ephemeral = false;
    };

    std::list<Node> nodes_queue;
    nodes_queue.emplace_back(
        from_path, from_zookeeper.asyncGet(from_path), from_zookeeper.asyncGetChildren(from_path), nullptr);

    to_zookeeper.createAncestors(to_path);

    for (auto it = nodes_queue.begin(); it != nodes_queue.end(); ++it)
    {
        Coordination::GetResponse get_response;
        Coordination::ListResponse children_response;
        try
        {
            get_response = it->get_future.get();
            children_response = it->children_future.get();
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::ZNONODE)
            {
                it->deleted = true;
                continue;
            }
            throw;
        }

        if (get_response.stat.ephemeralOwner)
        {
            it->ephemeral = true;
            continue;
        }

        if (it->parent && !it->parent->created)
        {
            it->parent->create_future.get();
            it->parent->created = true;
            std::cerr << it->parent->path << " copied!" << std::endl;
        }

        std::string new_path = it->path;
        new_path.replace(0, from_path.length(), to_path);
        it->create_future = to_zookeeper.asyncCreate(new_path, get_response.data, zkutil::CreateMode::Persistent);
        get_response.data.clear();
        get_response.data.shrink_to_fit();

        for (const auto & name : children_response.names)
        {
            std::string child_path = it->path == "/" ? it->path + name : it->path + '/' + name;
            nodes_queue.emplace_back(
                child_path, from_zookeeper.asyncGet(child_path), from_zookeeper.asyncGetChildren(child_path),
                &(*it));
        }
    }

    for (auto it = nodes_queue.begin(); it != nodes_queue.end(); ++it)
    {
        if (!it->created && !it->deleted && !it->ephemeral)
        {
            it->create_future.get();
            it->created = true;
            std::cerr << it->path << " copied!" << std::endl;
        }
    }
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
