#include "JSONFuzzer.h"
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include <boost/program_options.hpp>
#include <Common/TerminalSize.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int INCORRECT_DATA;
    }
}

void runJSONFuzzer(
    const JSONFuzzer::Config & config, DB::ReadBuffer & in,
    DB::WriteBuffer & out, size_t num_runs)
{
    String json;
    readJSONObjectPossiblyInvalid(json, in);

    JSONFuzzer fuzzer(config);
    while (num_runs--)
    {
        json = fuzzer.fuzz(json);
        DB::writeString(json, out);
        DB::writeString("\n", out);
    }
}

int main(int argc, char **argv)
try
{
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help", "produce help message")
        ("input-file", value<std::string>()->default_value(""), "")
        ("output-file", value<std::string>()->default_value(""), "")
        ("num-runs,n", value<size_t>()->default_value(1), "")
        ("max-added-children", value<size_t>()->default_value(10), "")
        ("max-array-size", value<size_t>()->default_value(10), "")
        ("max-string-size", value<size_t>()->default_value(10), "")
        ("random-seed", value<size_t>()->default_value(randomSeed()), "")
        ("verbose", "")
    ;

    po::variables_map options;
    po::store(boost::program_options::parse_command_line(argc, argv, desc), options);
    po::notify(options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options] < queries.txt\n";
        std::cout << desc << "\n";
        return 1;
    }

    JSONFuzzer::Config config
    {
        .max_added_children = options["max-added-children"].as<size_t>(),
        .max_array_size = options["max-array-size"].as<size_t>(),
        .max_string_size = options["max-string-size"].as<size_t>(),
        .random_seed = options["random-seed"].as<size_t>(),
        .verbose = options.contains("verbose"),
    };

    std::unique_ptr<DB::ReadBuffer> in;
    std::unique_ptr<DB::WriteBuffer> out;

    std::string input_file = options["input-file"].as<std::string>();
    std::string output_file = options["output-file"].as<std::string>();

    if (!input_file.empty())
        in = std::make_unique<DB::ReadBufferFromFile>(input_file);
    else
        in = std::make_unique<DB::ReadBufferFromFileDescriptor>(STDIN_FILENO);

    if (!output_file.empty())
        out = std::make_unique<DB::WriteBufferFromFile>(output_file);
    else
        out = std::make_unique<DB::WriteBufferFromFileDescriptor>(STDOUT_FILENO);

    size_t num_runs = options["num-runs"].as<size_t>();
    runJSONFuzzer(config, *in, *out, num_runs);
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    return 1;
}
