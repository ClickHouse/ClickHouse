#include <iostream>
#include <boost/program_options.hpp>
#include "Runner.h"
#include "Stats.h"
#include "Generator.h"
#include <Common/TerminalSize.h>
#include <Core/Types.h>

using namespace std;

int main(int argc, char *argv[])
{

    bool print_stacktrace = true;

    try
    {
        using boost::program_options::value;

        boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        desc.add_options()
            ("help",                                                            "produce help message")
            ("generator",     value<std::string>()->default_value("create_small_data"),             "query to execute")
            ("concurrency,c", value<unsigned>()->default_value(1),              "number of parallel queries")
            ("delay,d",       value<double>()->default_value(1),                "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("iterations,i",  value<size_t>()->default_value(0),                "amount of queries to be executed")
            ("timelimit,t",   value<double>()->default_value(0.),               "stop launch of queries after specified time limit")
            ("hosts,h",        value<Strings>()->multitoken(),                   "")
            ("continue_on_errors", "continue testing even if a query fails")
            ("reconnect", "establish new connection for every query")
        ;

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
        boost::program_options::notify(options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < queries.txt\n";
            std::cout << desc << "\n";
            return 1;
        }

        Runner runner(options["concurrency"].as<unsigned>(),
            options["generator"].as<std::string>(),
            options["hosts"].as<Strings>(),
            options["timelimit"].as<double>(),
            options["delay"].as<double>(),
            options.count("continue_on_errors"),
            options["iterations"].as<size_t>());

        runner.runBenchmark();

        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return DB::getCurrentExceptionCode();
    }
}
