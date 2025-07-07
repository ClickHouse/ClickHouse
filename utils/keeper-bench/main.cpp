#include <iostream>
#include <boost/program_options.hpp>
#include "Runner.h"
#include "Common/Exception.h"
#include <Common/TerminalSize.h>
#include <Core/Types.h>
#include <boost/program_options/variables_map.hpp>

namespace
{

template <typename T>
std::optional<T> valueToOptional(const boost::program_options::variable_value & value)
{
    if (value.empty())
        return std::nullopt;

    return value.as<T>();
}

}

int main(int argc, char *argv[])
{

    bool print_stacktrace = true;

    //Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    //Poco::Logger::root().setChannel(channel);
    //Poco::Logger::root().setLevel("trace");

    try
    {
        using boost::program_options::value;

        boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        desc.add_options()
            ("help",                                                                         "produce help message")
            ("config",            value<std::string>()->default_value(""),                      "yaml/xml file containing configuration")
            ("input-request-log", value<std::string>()->default_value(""),                      "log of requests that will be replayed")
            ("setup-nodes-snapshot-path", value<std::string>()->default_value(""),                      "directory containing snapshots with starting state")
            ("concurrency,c",     value<unsigned>(),                                            "number of parallel queries")
            ("report-delay,d",    value<double>(),                                              "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("iterations,i",      value<size_t>(),                                              "amount of queries to be executed")
            ("time-limit,t",      value<double>(),                                              "stop launch of queries after specified time limit")
            ("hosts,h",           value<Strings>()->multitoken()->default_value(Strings{}, ""), "")
            ("continue_on_errors", "continue testing even if a query fails")
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

        Runner runner(valueToOptional<unsigned>(options["concurrency"]),
                      options["config"].as<std::string>(),
                      options["input-request-log"].as<std::string>(),
                      options["setup-nodes-snapshot-path"].as<std::string>(),
                      options["hosts"].as<Strings>(),
                      valueToOptional<double>(options["time-limit"]),
                      valueToOptional<double>(options["report-delay"]),
                      options.count("continue_on_errors") ? std::optional<bool>(true) : std::nullopt,
                      valueToOptional<size_t>(options["iterations"]));

        try
        {
            runner.runBenchmark();
        }
        catch (...)
        {
            std::cout << "Got exception while trying to run benchmark: " << DB::getCurrentExceptionMessage(true) << std::endl;
        }

        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return DB::getCurrentExceptionCode();
    }
}
