#pragma once
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include "Generator.h"
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/randomSeed.h>

using Ports = std::vector<UInt16>;

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
    Runner(size_t concurrency_, const std::string & generator_name,
           const Strings & hosts_strings_,
           bool continue_on_error_)
        : concurrency(concurrency_)
        , pool(concurrency)
        , hosts_strings(hosts_strings_)
        , generator(generator_name == "create_no_data" ? nullptr : std::make_unique<CreateRequestGenerator>("/"))
        , continue_on_error(continue_on_error_)
        , queue(concurrency)
    {

    }

    void thread(std::vector<Coordination::ZooKeeper> & zookeepers)
    {
        ZooKeeperRequestPtr request;
        /// Randomly choosing connection index
        pcg64 rng(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, zookeepers.size() - 1);

        /// In these threads we do not accept INT signal.
        sigset_t sig_set;
        if (sigemptyset(&sig_set)
            || sigaddset(&sig_set, SIGINT)
            || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        {
            throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);
        }

        while (true)
        {
            bool extracted = false;

            while (!extracted)
            {
                extracted = queue.tryPop(request, 100);

                if (shutdown
                    || (max_iterations && requests_executed == max_iterations))
                {
                    return;
                }
            }

            const auto connection_index = distribution(generator);
            auto & zk = zookeepers[connection_index];

            auto promise = std::make_shared<std::promise<void>>();
            auto future = promise->get_future();
            ResponseCallback callback = [promise](const Response & response)
            {
                if (response.error != Coordination::Error::OK)
                    promise->set_exception(std::make_exception_ptr(KeeperException(response.error)));
                else
                    promise->set_value();
            };

            Stopwatch watch;

            zk.executeGenericRequest(request, callback);

            try
            {
                future.get();
                double seconds = watch.elapsedSeconds();
            }
            catch (...)
            {
                if (!continue_on_error)
                {
                    shutdown = true;
                    throw;
                }
                std::cerr << getCurrentExceptionMessage(true,
                        true /*check embedded stack trace*/) << std::endl;
            }

            ++requests_executed;
        }

    }


private:

    size_t concurrency;

    ThreadPool pool;
    Strings hosts_strings;
    std::unique_ptr<IGenerator> generator;
    bool continue_on_error;
    std::atomic<size_t> max_iterations;
    std::atomic<size_t> requests_executed;
    std::atomic<bool> shutdown;

    using Queue = ConcurrentBoundedQueue<ZooKeeperRequestPtr>;
    Queue queue;

    std::vector<Coordination::ZooKeeper> getConnections()
    {
        std::vector<Coordination::ZooKeeper> zookeepers;
        for (const auto & host_string : hosts_strings)
        {
            Coordination::ZooKeeper::Node node{Poco::Net::SocketAddress{host_string}, false};
            std::vector<Coordination::ZooKeeper::Node> nodes;
            nodes.push_back(node);
            zookeepers.emplace_back(
                nodes,
                "/",
                "",
                "",
                Poco::Timespan(0, 30000 * 1000),
                Poco::Timespan(0, 1000 * 1000),
                Poco::Timespan(0, 10000 * 1000));
        }

        return zookeepers;
    }
};
