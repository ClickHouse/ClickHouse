#include "Stats.h"
#include <iostream>

void report(std::shared_ptr<Stats> & info, size_t concurrency)
{
    std::cerr << "\n";

    /// Avoid zeros, nans or exceptions
    if (0 == info->read_requests && 0 == info->write_requests)
        return;

    double read_seconds = info->read_work_time / concurrency;
    double write_seconds = info->write_work_time / concurrency;

    std::cerr << "read requests " << info->read_requests << ", write requests " << info->write_requests << ", ";
    if (info->errors)
    {
        std::cerr << "errors " << info->errors << ", ";
    }
    if (0 != info->read_requests)
    {
        std::cerr
            << "Read RPS: " << (info->read_requests / read_seconds) << ", "
            << "Read MiB/s: " << (info->requests_read_bytes / read_seconds / 1048576);
        if (0 != info->write_requests)
            std::cerr << ", ";
    }
    if (0 != info->write_requests)
    {
        std::cerr
            << "Write RPS: " << (info->write_requests / write_seconds) << ", "
            << "Write MiB/s: " << (info->requests_write_bytes / write_seconds / 1048576) << ". "
            << "\n";
    }
    std::cerr << "\n";

    auto print_percentile = [&](double percent, Stats::Sampler & sampler)
    {
        std::cerr << percent << "%\t\t";
        std::cerr << sampler.quantileNearest(percent / 100.0) << " sec.\t";
        std::cerr << "\n";
    };

    if (0 != info->read_requests)
    {
        std::cerr << "Read sampler:\n";
        for (int percent = 0; percent <= 90; percent += 10)
            print_percentile(percent, info->read_sampler);

        print_percentile(95, info->read_sampler);
        print_percentile(99, info->read_sampler);
        print_percentile(99.9, info->read_sampler);
        print_percentile(99.99, info->read_sampler);
    }

    if (0 != info->write_requests)
    {
        std::cerr << "Write sampler:\n";
        for (int percent = 0; percent <= 90; percent += 10)
            print_percentile(percent, info->write_sampler);

        print_percentile(95, info->write_sampler);
        print_percentile(99, info->write_sampler);
        print_percentile(99.9, info->write_sampler);
        print_percentile(99.99, info->write_sampler);
    }
}
