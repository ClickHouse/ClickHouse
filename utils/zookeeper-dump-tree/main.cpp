#include <iostream>
#include <boost/program_options.hpp>
#include <Common/Exception.h>
#include <Poco/Event.h>
#include <Common/ZooKeeper/ZooKeeper.h>


/** Outputs paths of all ZK nodes in arbitrary order. Possibly only in specified directory.
  */

struct CallbackState
{
    std::string path;
    std::list<CallbackState>::const_iterator it;
    std::list<std::list<CallbackState>::const_iterator> children;
    Int64 dataLength = 0;
};

using CallbackStates = std::list<CallbackState>;
CallbackStates states;

zkutil::ZooKeeper * zookeeper;

int running_count = 0;
Poco::Event completed;


void process(CallbackState & state);

void callback(
    int rc,
    const String_vector * strings,
    const Stat * stat,
    const void * data)
{
    CallbackState * state = reinterpret_cast<CallbackState *>(const_cast<void *>(data));

    if (rc != ZOK && rc != ZNONODE)
    {
        std::cerr << zerror(rc) << ", path: " << state->path << "\n";
    }

    if (stat != nullptr)
        state->dataLength = stat->dataLength;

    if (rc == ZOK && strings)
    {
        for (int32_t i = 0; i < strings->count; ++i)
        {
            states.emplace_back();
            states.back().path = state->path + (state->path == "/" ? "" : "/") + strings->data[i];
            states.back().it = --states.end();
            state->children.push_back(states.back().it);

            process(states.back());
        }
    }

    --running_count;
    if (running_count == 0)
        completed.set();
}

void process(CallbackState & state)
{
    ++running_count;
    zoo_awget_children2(zookeeper->getHandle(), state.path.data(), nullptr, nullptr, callback, &state);
}

typedef std::pair<Int64, Int64> NodesBytes;

NodesBytes printTree(const CallbackState & state)
{
    Int64 nodes = 1;
    Int64 bytes = state.dataLength;
    for (auto child : state.children)
    {
        NodesBytes nodesBytes = printTree(*child);
        nodes += nodesBytes.first;
        bytes += nodesBytes.second;
    }
    std::cout << state.path << '\t' << nodes << '\t' << bytes <<'\n';
    return NodesBytes(nodes, bytes);
}

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

    zkutil::ZooKeeper zookeeper_(options.at("address").as<std::string>());
    zookeeper = &zookeeper_;

    states.emplace_back();
    states.back().path = options.at("path").as<std::string>();
    states.back().it = --states.end();

    process(states.back());

    completed.wait();

    printTree(*states.begin());
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
