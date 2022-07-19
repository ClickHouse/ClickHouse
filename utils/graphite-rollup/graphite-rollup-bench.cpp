#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <system_error>

#include <boost/program_options.hpp>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Processors/Merges/Algorithms/GraphiteRollupSortedAlgorithm.h>
#include <Storages/System/StorageSystemGraphite.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>

using namespace DB;

static SharedContextHolder shared_context = Context::createShared();

std::vector<StringRef> loadMetrics(const std::string & metrics_file)
{
    std::vector<StringRef> metrics;

    FILE * stream;
    char * line = nullptr;
    size_t len = 0;
    ssize_t nread;

    stream = fopen(metrics_file.c_str(), "r");
    if (stream == nullptr)
    {
        throw std::runtime_error(strerror(errno));
    }

    while ((nread = getline(&line, &len, stream)) != -1) /// NOLINT
    {
        size_t l = strlen(line);
        if (l > 0)
        {
            if (line[l - 1] == '\n')
            {
                line[l - 1] = '\0';
                l--;
            }
            if (l > 0)
            {
                metrics.push_back(StringRef(strdup(line), l));
            }
        }
    }
    free(line);
    if (ferror(stream))
    {
        fclose(stream);
        throw std::runtime_error(strerror(errno));
    }

    fclose(stream);

    return metrics;
}

ConfigProcessor::LoadedConfig loadConfiguration(const std::string & config_path)
{
    ConfigProcessor config_processor(config_path, true, true);
    ConfigProcessor::LoadedConfig config = config_processor.loadConfig(false);
    return config;
}

void bench(const std::string & config_path, const std::string & metrics_file, size_t n, bool verbose)
{
    auto config = loadConfiguration(config_path);

    auto context = Context::createGlobal(shared_context.get());
    context->setConfig(config.configuration.get());

    Graphite::Params params;
    setGraphitePatternsFromConfig(context, "graphite_rollup", params);

    std::vector<StringRef> metrics = loadMetrics(metrics_file);

    std::vector<double> durations(metrics.size());
    size_t j, i;
    for (j = 0; j < n; j++)
    {
        for (i = 0; i < metrics.size(); i++)
        {
            auto start = std::chrono::high_resolution_clock::now();

            auto rule = DB::Graphite::selectPatternForPath(params, metrics[i]);
            (void)rule;

            auto end = std::chrono::high_resolution_clock::now();
            double duration = (duration_cast<std::chrono::duration<double>>(end - start)).count() * 1E9;
            durations[i] += duration;

            if (j == 0 && verbose)
            {
                std::cout << metrics[i].data << ": rule with regexp '" << rule.second->regexp_str << "' found\n";
            }
        }
    }

    for (i = 0; i < metrics.size(); i++)
    {
        std::cout << metrics[i].data << " " << durations[i] / n << " ns\n";
        free(const_cast<void *>(static_cast<const void *>(metrics[i].data)));
    }
}

int main(int argc, char ** argv)
{
    registerAggregateFunctions();

    std::string config_file, metrics_file;

    using namespace std::literals;

    std::string config_default = RULES_DIR + "/rollup.xml"s;
    std::string metrics_default = RULES_DIR + "/metrics.txt"s;

    namespace po = boost::program_options;
    po::variables_map vm;

    po::options_description desc;
    desc.add_options()("help,h", "produce help")(
        "config,c", po::value<std::string>()->default_value(config_default), "XML config with rollup rules")(
        "metrics,m", po::value<std::string>()->default_value(metrics_default), "metrcis files (one metric per line) for run benchmark")(
        "verbose,V", po::bool_switch()->default_value(false), "verbose output (print found rule)");

    po::parsed_options parsed = po::command_line_parser(argc, argv).options(desc).run();
    po::store(parsed, vm);
    po::notify(vm);

    if (vm.count("help"))
    {
        std::cout << desc << '\n';
        exit(1);
    }

    bench(vm["config"].as<std::string>(), vm["metrics"].as<std::string>(), 10000, vm["verbose"].as<bool>());

    return 0;
}
