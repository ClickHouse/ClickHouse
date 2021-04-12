#pragma once

#include <vector>
#include <atomic>

#include <AggregateFunctions/ReservoirSampler.h>

struct Stats
{
    std::atomic<size_t> requests{0};
    size_t errors = 0;
    size_t requests_write_bytes = 0;
    size_t requests_read_bytes = 0;
    double work_time = 0;

    using Sampler = ReservoirSampler<double>;
    Sampler sampler {1 << 16};

    void add(double seconds, size_t request_read_bytes_inc, size_t request_write_bytes_inc)
    {
        ++requests;
        work_time += seconds;
        requests_read_bytes += request_read_bytes_inc;
        requests_write_bytes += request_write_bytes_inc;
        sampler.insert(seconds);
    }

    void clear()
    {
        requests = 0;
        work_time = 0;
        requests_read_bytes = 0;
        requests_write_bytes = 0;
        sampler.clear();
    }
};

using MultiStats = std::vector<std::shared_ptr<Stats>>;

void report(MultiStats & infos, size_t concurrency, bool cumulative);
