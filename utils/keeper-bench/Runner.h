#pragma once
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include "Generator.h"
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Common/InterruptListener.h>

#include <Core/Types.h>
#include "Stats.h"

using Ports = std::vector<UInt16>;
using Strings = std::vector<std::string>;

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int EMPTY_DATA_PASSED;
}
}

class Runner
{
public:
    Runner(
        size_t concurrency_,
        const std::string & generator_name,
        const Strings & hosts_strings_,
        double max_time_,
        double delay_,
        bool continue_on_error_,
        size_t max_iterations_)
        : concurrency(concurrency_)
        , pool(concurrency)
        , hosts_strings(hosts_strings_)
        , generator(generator_name == "create_no_data" ? std::make_unique<CreateRequestGenerator>("/") : nullptr)
        , max_time(max_time_)
        , delay(delay_)
        , continue_on_error(continue_on_error_)
        , max_iterations(max_iterations_)
        , info(std::make_shared<Stats>())
        , queue(concurrency)
    {
    }

    void thread(std::vector<std::shared_ptr<Coordination::ZooKeeper>> & zookeepers)
    {
        Coordination::ZooKeeperRequestPtr request;
        /// Randomly choosing connection index
        pcg64 rng(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, zookeepers.size() - 1);

        /// In these threads we do not accept INT signal.
        sigset_t sig_set;
        if (sigemptyset(&sig_set)
            || sigaddset(&sig_set, SIGINT)
            || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        {
            DB::throwFromErrno("Cannot block signal.", DB::ErrorCodes::CANNOT_BLOCK_SIGNAL);
        }

        while (true)
        {
            bool extracted = false;

            while (!extracted)
            {
                extracted = queue.tryPop(request, 100);

                if (shutdown
                    || (max_iterations && requests_executed >= max_iterations))
                {
                    return;
                }
            }

            const auto connection_index = distribution(rng);
            auto & zk = zookeepers[connection_index];

            auto promise = std::make_shared<std::promise<void>>();
            auto future = promise->get_future();
            Coordination::ResponseCallback callback = [promise](const Coordination::Response & response)
            {
                if (response.error != Coordination::Error::ZOK)
                    promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
                else
                    promise->set_value();
            };

            Stopwatch watch;

            zk->executeGenericRequest(request, callback);

            try
            {
                future.get();
                double seconds = watch.elapsedSeconds();

                std::lock_guard lock(mutex);

                info->add(seconds, 0, 0);
            }
            catch (...)
            {
                if (!continue_on_error)
                {
                    shutdown = true;
                    throw;
                }
                std::cerr << DB::getCurrentExceptionMessage(true, true /*check embedded stack trace*/) << std::endl;
            }

            ++requests_executed;
        }

    }
    void printNumberOfRequestsExecuted(size_t num)
    {
        std::cerr << "Requests executed: " << num << ".\n";
    }

    bool tryPushRequestInteractively(const Coordination::ZooKeeperRequestPtr & request, DB::InterruptListener & interrupt_listener)
    {
        bool inserted = false;

        while (!inserted)
        {
            inserted = queue.tryPush(request, 100);

            if (shutdown)
            {
                /// An exception occurred in a worker
                return false;
            }

            if (max_time > 0 && total_watch.elapsedSeconds() >= max_time)
            {
                std::cout << "Stopping launch of queries. Requested time limit is exhausted.\n";
                return false;
            }

            if (interrupt_listener.check())
            {
                std::cout << "Stopping launch of queries. SIGINT received." << std::endl;
                return false;
            }

            if (delay > 0 && delay_watch.elapsedSeconds() > delay)
            {
                printNumberOfRequestsExecuted(requests_executed);

                std::lock_guard lock(mutex);
                report(info, concurrency);
                delay_watch.restart();
            }
        }

        return true;
    }

    void runBenchmark()
    {
        try
        {
            for (size_t i = 0; i < concurrency; ++i)
            {
                auto connections = getConnections();
                pool.scheduleOrThrowOnError([this, connections]() mutable { thread(connections); });
            }
        }
        catch (...)
        {
            pool.wait();
            throw;
        }

        DB::InterruptListener interrupt_listener;
        delay_watch.restart();

        /// Push queries into queue
        for (size_t i = 0; !max_iterations || i < max_iterations; ++i)
        {
            if (!tryPushRequestInteractively(generator->generate(), interrupt_listener))
            {
                shutdown = true;
                break;
            }
        }

        pool.wait();
        total_watch.stop();

        printNumberOfRequestsExecuted(requests_executed);

        std::lock_guard lock(mutex);
        report(info, concurrency);
    }


private:

    size_t concurrency;

    ThreadPool pool;
    Strings hosts_strings;
    std::unique_ptr<IGenerator> generator;
    double max_time;
    double delay;
    bool continue_on_error;
    std::atomic<size_t> max_iterations;
    std::atomic<size_t> requests_executed;
    std::atomic<bool> shutdown;

    std::shared_ptr<Stats> info;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    std::mutex mutex;

    using Queue = ConcurrentBoundedQueue<Coordination::ZooKeeperRequestPtr>;
    Queue queue;

    std::vector<std::shared_ptr<Coordination::ZooKeeper>> getConnections()
    {
        std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers;
        for (const auto & host_string : hosts_strings)
        {
            Coordination::ZooKeeper::Node node{Poco::Net::SocketAddress{host_string}, false};
            std::vector<Coordination::ZooKeeper::Node> nodes;
            nodes.push_back(node);
            zookeepers.emplace_back(std::make_shared<Coordination::ZooKeeper>(
                nodes,
                "/",
                "",
                "",
                Poco::Timespan(0, 30000 * 1000),
                Poco::Timespan(0, 1000 * 1000),
                Poco::Timespan(0, 10000 * 1000)));
        }

        return zookeepers;
    }
};
