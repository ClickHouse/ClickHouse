#include <iostream>
#include <random>
#include <pcg_random.hpp>

#include <boost/program_options.hpp>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>

#include "MarkovModel.h"

using namespace DB;


int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("create", "create model")
        ("order", boost::program_options::value<unsigned>(), "order of model to create")
        ("noise", boost::program_options::value<double>(), "relative random noise to apply to created model")
        ("generate", "generate random strings with model")
        ("max-string-size", boost::program_options::value<UInt64>()->default_value(10000), "maximum size of generated string")
        ("limit", boost::program_options::value<UInt64>(), "stop after specified count of generated strings")
        ("seed", boost::program_options::value<UInt64>(), "seed passed to random number generator")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    auto show_usage = [&]
    {
        std::cout << "Usage: \n"
            << argv[0] << " --create --order=N < strings.tsv > model\n"
            << argv[0] << " --generate < model > strings.tsv\n\n";
        std::cout << desc << std::endl;
    };

    if (options.count("help"))
    {
        show_usage();
        return 1;
    }

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    pcg64 random;

    if (options.count("seed"))
        random.seed(options["seed"].as<UInt64>());

    if (options.count("create"))
    {
        MarkovModel model(options["order"].as<unsigned>());

        String s;
        while (!in.eof())
        {
            readText(s, in);
            assertChar('\n', in);

            model.consume(s.data(), s.size());
        }

        if (options.count("noise"))
        {
            double noise = options["noise"].as<double>();
            model.modifyCounts([&](UInt32 count)
            {
                double modified = std::normal_distribution<double>(count, count * noise)(random);
                if (modified < 1)
                    modified = 1;

                return std::round(modified);
            });
        }

        model.write(out);
    }
    else if (options.count("generate"))
    {
        MarkovModel model;
        model.read(in);
        String s;

        UInt64 limit = options.count("limit") ? options["limit"].as<UInt64>() : 0;
        UInt64 max_string_size = options["max-string-size"].as<UInt64>();

        for (size_t i = 0; limit == 0 || i < limit; ++i)
        {
            s.resize(max_string_size);
            s.resize(model.generate(&s[0], s.size(), [&]{ return random(); }));

            writeText(s, out);
            writeChar('\n', out);
        }
    }
    else
    {
        show_usage();
        return 1;
    }

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
