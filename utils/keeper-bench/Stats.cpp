#include "Stats.h"
#include <iostream>

void report(MultiStats & infos, size_t concurrency, bool cumulative)
{
    std::cerr << "\n";
    for (size_t i = 0; i < infos.size(); ++i)
    {
        const auto & info = infos[i];

        /// Avoid zeros, nans or exceptions
        if (0 == info->queries)
            return;

        double seconds = info->work_time / concurrency;

        std::cerr
                << "connection " << i << ", "
                << "queries " << info->queries << ", ";
        if (info->errors)
        {
            std::cerr << "errors " << info->errors << ", ";
        }
        std::cerr
                << "RPS: " << (info->requests / seconds) << ", "
                << "Read MiB/s: " << (info->requests_read_bytes / seconds / 1048576) << ", "
                << "Write MiB/s: " << (info->requests_write_bytes / seconds / 1048576) << ". "
                << "\n";
    }
    std::cerr << "\n";

    auto print_percentile = [&](double percent)
    {
        std::cerr << percent << "%\t\t";
        for (const auto & info : infos)
        {
            std::cerr << info->sampler.quantileNearest(percent / 100.0) << " sec.\t";
        }
        std::cerr << "\n";
    };

    for (int percent = 0; percent <= 90; percent += 10)
        print_percentile(percent);

    print_percentile(95);
    print_percentile(99);
    print_percentile(99.9);
    print_percentile(99.99);

    if (!cumulative)
    {
        for (auto & info : infos)
            info->clear();
    }
}
