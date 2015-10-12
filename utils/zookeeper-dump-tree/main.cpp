#include <iostream>
#include <boost/program_options.hpp>
#include <DB/Common/Exception.h>
#include <Poco/Event.h>
#include <zkutil/ZooKeeper.h>


/** Выводит в произвольном порядке пути всех узлов ZK. Возможно, только в указанной директории.
  */

struct CallbackState
{
	std::string path;
	std::list<CallbackState>::const_iterator it;
};

using CallbackStates = std::list<CallbackState>;
CallbackStates states;

zkutil::ZooKeeper * zookeeper;

Poco::Event completed;


void process(const CallbackState & state);

void callback(
	int rc,
	const String_vector * strings,
	const Stat * stat,
	const void * data)
{
	const CallbackState * state = reinterpret_cast<const CallbackState *>(data);

	if (rc != ZOK && rc != ZNONODE)
	{
		std::cerr << zerror(rc) << ", path: " << state->path << "\n";
	}

	if (rc == ZOK && strings)
	{
		for (int32_t i = 0; i < strings->count; ++i)
		{
			states.emplace_back();
			states.back().path = state->path + (state->path == "/" ? "" : "/") + strings->data[i];
			states.back().it = --states.end();

			std::cout << states.back().path << '\n';
			process(states.back());
		}
	}

	states.erase(state->it);

	if (states.empty())
		completed.set();
}

void process(const CallbackState & state)
{
	zoo_awget_children2(zookeeper->getHandle(), state.path.data(), nullptr, nullptr, callback, &state);
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
}
catch (...)
{
	std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
	throw;
}
