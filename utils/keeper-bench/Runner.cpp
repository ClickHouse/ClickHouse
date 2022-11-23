#include "Runner.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_BLOCK_SIGNAL;
}

}


void Runner::thread(std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers)
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

        auto promise = std::make_shared<std::promise<size_t>>();
        auto future = promise->get_future();
        Coordination::ResponseCallback callback = [promise](const Coordination::Response & response)
        {
            if (response.error != Coordination::Error::ZOK)
                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
            else
                promise->set_value(response.bytesSize());
        };

        Stopwatch watch;

        zk->executeGenericRequest(request, callback);

        try
        {
            auto response_size = future.get();
            double seconds = watch.elapsedSeconds();

            std::lock_guard lock(mutex);

            if (request->isReadRequest())
                info->addRead(seconds, 1, request->bytesSize() + response_size);
            else
                info->addWrite(seconds, 1, request->bytesSize() + response_size);
        }
        catch (...)
        {
            if (!continue_on_error)
            {
                shutdown = true;
                throw;
            }
            std::cerr << DB::getCurrentExceptionMessage(true, true /*check embedded stack trace*/) << std::endl;

            bool got_expired = false;
            for (const auto & connection : zookeepers)
            {
                if (connection->isExpired())
                {
                    got_expired = true;
                    break;
                }
            }
            if (got_expired)
            {
                while (true)
                {
                    try
                    {
                        zookeepers = getConnections();
                        break;
                    }
                    catch (...)
                    {
                        std::cerr << DB::getCurrentExceptionMessage(true, true /*check embedded stack trace*/) << std::endl;
                    }
                }
            }
        }

        ++requests_executed;
    }
}

bool Runner::tryPushRequestInteractively(const Coordination::ZooKeeperRequestPtr & request, DB::InterruptListener & interrupt_listener)
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


void Runner::runBenchmark()
{
    auto aux_connections = getConnections();

    std::cerr << "Preparing to run\n";
    generator->startup(*aux_connections[0]);
    std::cerr << "Prepared\n";
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
        shutdown = true;
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


std::vector<std::shared_ptr<Coordination::ZooKeeper>> Runner::getConnections()
{
    std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers;
    for (const auto & host_string : hosts_strings)
    {
        Coordination::ZooKeeper::Node node{Poco::Net::SocketAddress{host_string}, false};
        std::vector<Coordination::ZooKeeper::Node> nodes;
        nodes.push_back(node);
        zookeepers.emplace_back(std::make_shared<Coordination::ZooKeeper>(
            nodes,
            "", /*chroot*/
            "", /*identity type*/
            "", /*identity*/
            Poco::Timespan(0, 30000 * 1000),
            Poco::Timespan(0, 1000 * 1000),
            Poco::Timespan(0, 10000 * 1000),
            nullptr));

    }


    return zookeepers;
}
