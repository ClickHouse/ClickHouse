#include <list>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <boost/program_options.hpp>


int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("address,a", boost::program_options::value<std::string>()->required(),
        "addresses of ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
        ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Remove nodes in ZooKeeper by list passed in stdin." << std::endl;
        std::cout << "Usage: " << argv[0] << " [options] < list_of_nodes_on_each_line" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    zkutil::ZooKeeper zookeeper(options.at("address").as<std::string>());

    DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
    std::list<std::future<Coordination::RemoveResponse>> futures;

    std::cerr << "Requested: ";
    while (!in.eof())
    {
        std::string path;
        DB::readEscapedString(path, in);
        DB::assertString("\n", in);
        futures.emplace_back(zookeeper.asyncRemove(path));
        std::cerr << ".";
    }
    std::cerr << "\n";

    std::cerr << "Done: ";
    for (auto & future : futures)
    {
        try
        {
            future.get();
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        }
        std::cerr << ".";
    }
    std::cerr << "\n";

    return 0;
}
catch (const Poco::Exception & e)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
