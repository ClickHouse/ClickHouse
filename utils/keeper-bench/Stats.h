#pragma once

#include <vector>
#include <atomic>

#include <AggregateFunctions/ReservoirSampler.h>

struct Stats
{
    std::atomic<size_t> read_requests{0};
    std::atomic<size_t> write_requests{0};
    size_t errors = 0;
    size_t requests_write_bytes = 0;
    size_t requests_read_bytes = 0;
    double read_work_time = 0;
    double write_work_time = 0;

    using Sampler = ReservoirSampler<double>;
    Sampler read_sampler {1 << 16};
    Sampler write_sampler {1 << 16};

    void addRead(double seconds, size_t requests_inc, size_t bytes_inc)
    {
        read_work_time += seconds;
        read_requests += requests_inc;
        requests_read_bytes += bytes_inc;
        read_sampler.insert(seconds);
    }

    void addWrite(double seconds, size_t requests_inc, size_t bytes_inc)
    {
        write_work_time += seconds;
        write_requests += requests_inc;
        requests_write_bytes += bytes_inc;
        write_sampler.insert(seconds);
    }

    void clear()
    {
        read_requests = 0;
        write_requests = 0;
        read_work_time = 0;
        write_work_time = 0;
        requests_read_bytes = 0;
        requests_write_bytes = 0;
        read_sampler.clear();
        write_sampler.clear();
    }
};


void report(std::shared_ptr<Stats> & info, size_t concurrency);
