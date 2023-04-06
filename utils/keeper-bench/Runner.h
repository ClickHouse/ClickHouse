#pragma once
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include "Generator.h"
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Common/InterruptListener.h>
#include <Common/CurrentMetrics.h>

#include <Core/Types.h>
#include "Stats.h"

using Ports = std::vector<UInt16>;
using Strings = std::vector<std::string>;

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class Runner
{
public:
    Runner(
        size_t concurrency_,
        const std::string & generator_name,
        const std::string & config_path,
        const Strings & hosts_strings_,
        double max_time_,
        double delay_,
        bool continue_on_error_,
        size_t max_iterations_);

    void thread(std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers);

    void printNumberOfRequestsExecuted(size_t num)
    {
        std::cerr << "Requests executed: " << num << ".\n";
    }

    bool tryPushRequestInteractively(const Coordination::ZooKeeperRequestPtr & request, DB::InterruptListener & interrupt_listener);

    void runBenchmark();


private:

    size_t concurrency = 1;

    ThreadPool pool;
    Strings hosts_strings;
    std::unique_ptr<Generator> generator;
    double max_time = 0;
    double delay = 1;
    bool continue_on_error = false;
    std::atomic<size_t> max_iterations = 0;
    std::atomic<size_t> requests_executed = 0;
    std::atomic<bool> shutdown = false;

    std::shared_ptr<Stats> info;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    std::mutex mutex;

    using Queue = ConcurrentBoundedQueue<Coordination::ZooKeeperRequestPtr>;
    Queue queue;

    std::vector<std::shared_ptr<Coordination::ZooKeeper>> getConnections();
};
