#include "Runner.h"
#include <Poco/Util/AbstractConfiguration.h>

#include "Common/ZooKeeper/ZooKeeperCommon.h"
#include "Common/ZooKeeper/ZooKeeperConstants.h"
#include <Common/Config/ConfigProcessor.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_BLOCK_SIGNAL;
}

Runner::Runner(
        size_t concurrency_,
        const std::string & generator_name,
        const std::string & config_path,
        const Strings & hosts_strings_,
        double max_time_,
        double delay_,
        bool continue_on_error_,
        size_t max_iterations_)
        : concurrency(concurrency_)
        , pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, concurrency)
        , max_time(max_time_)
        , delay(delay_)
        , continue_on_error(continue_on_error_)
        , max_iterations(max_iterations_)
        , info(std::make_shared<Stats>())
        , queue(concurrency)
{

    DB::ConfigurationPtr config = nullptr;

    if (!config_path.empty())
    {
        DB::ConfigProcessor config_processor(config_path, true, false);
        config = config_processor.loadConfig().configuration;
    }

    if (!generator_name.empty())
    {
        generator = getGenerator(generator_name);

        if (!generator)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to create generator");
    }
    else
    {
        if (!config)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No config file or generator name defined");

        generator = std::make_unique<Generator>(*config);
    }

    if (!hosts_strings_.empty())
    {
        for (const auto & host : hosts_strings_)
            connection_infos.push_back({.host = host});
    }
    else
    {
        if (!config)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No config file or hosts defined");

        parseHostsFromConfig(*config);
    }
}

void Runner::parseHostsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    ConnectionInfo default_connection_info;

    const auto fill_connection_details = [&](const std::string & key, auto & connection_info)
    {
        if (config.has(key + ".secure"))
            connection_info.secure = config.getBool(key + ".secure");

        if (config.has(key + ".session_timeout_ms"))
            connection_info.session_timeout_ms = config.getInt(key + ".session_timeout_ms");

        if (config.has(key + ".operation_timeout_ms"))
            connection_info.operation_timeout_ms = config.getInt(key + ".operation_timeout_ms");

        if (config.has(key + ".connection_timeout_ms"))
            connection_info.connection_timeout_ms = config.getInt(key + ".connection_timeout_ms");
    };

    fill_connection_details("connections", default_connection_info);

    Poco::Util::AbstractConfiguration::Keys connections_keys;
    config.keys("connections", connections_keys);

    for (const auto & key : connections_keys)
    {
        std::string connection_key = "connections." + key;
        auto connection_info = default_connection_info;
        if (key.starts_with("host"))
        {
            connection_info.host = config.getString(connection_key);
            connection_infos.push_back(std::move(connection_info));
        }
        else if (key.starts_with("connection") && key != "connection_timeout_ms")
        {
            connection_info.host = config.getString(connection_key + ".host");
            if (config.has(connection_key + ".sessions"))
                connection_info.sessions = config.getUInt64(connection_key + ".sessions");

            fill_connection_details(connection_key, connection_info);

            connection_infos.push_back(std::move(connection_info));
        }
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
                        zookeepers = refreshConnections();
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
    static std::unordered_map<Coordination::OpNum, size_t> counts;
    static size_t i = 0;
    
    counts[request->getOpNum()]++;

    //if (request->getOpNum() == Coordination::OpNum::Multi)
    //{
    //    for (const auto & multi_request : dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*request).requests)
    //        counts[dynamic_cast<Coordination::ZooKeeperRequest &>(*multi_request).getOpNum()]++;
    //}

    ++i;
    if (i % 10000 == 0)
    {
        for (const auto & [op_num, count] : counts)
            std::cout << fmt::format("{}: {}", op_num, count) << std::endl;
    }

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
    createConnections();

    std::cerr << "Preparing to run\n";
    generator->startup(*connections[0]);
    std::cerr << "Prepared\n";
    try
    {
        for (size_t i = 0; i < concurrency; ++i)
        {
            auto thread_connections = connections;
            pool.scheduleOrThrowOnError([this, connections = std::move(thread_connections)]() mutable { thread(connections); });
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


void Runner::createConnections()
{
    for (size_t connection_info_idx = 0; connection_info_idx < connection_infos.size(); ++connection_info_idx)
    {
        const auto & connection_info = connection_infos[connection_info_idx];
        std::cout << fmt::format("Creating {} session(s) for:\n"
                                 "- host: {}\n"
                                 "- secure: {}\n"
                                 "- session timeout: {}ms\n"
                                 "- operation timeout: {}ms\n"
                                 "- connection timeout: {}ms",
                                 connection_info.sessions,
                                 connection_info.host,
                                 connection_info.secure,
                                 connection_info.session_timeout_ms,
                                 connection_info.operation_timeout_ms,
                                 connection_info.connection_timeout_ms) << std::endl;

        for (size_t session = 0; session < connection_info.sessions; ++session)
        {
            connections.emplace_back(getConnection(connection_info));
            connections_to_info_map[connections.size() - 1] = connection_info_idx;
        }
    }
}

std::shared_ptr<Coordination::ZooKeeper> Runner::getConnection(const ConnectionInfo & connection_info)
{
    Coordination::ZooKeeper::Node node{Poco::Net::SocketAddress{connection_info.host}, connection_info.secure};
    std::vector<Coordination::ZooKeeper::Node> nodes;
    nodes.push_back(node);
    zkutil::ZooKeeperArgs args;
    args.session_timeout_ms = connection_info.session_timeout_ms;
    args.connection_timeout_ms = connection_info.operation_timeout_ms;
    args.operation_timeout_ms = connection_info.connection_timeout_ms;
    return std::make_shared<Coordination::ZooKeeper>(nodes, args, nullptr);
}

std::vector<std::shared_ptr<Coordination::ZooKeeper>> Runner::refreshConnections()
{
    std::lock_guard lock(connection_mutex);
    for (size_t connection_idx = 0; connection_idx < connections.size(); ++connection_idx)
    {
        auto & connection = connections[connection_idx];
        if (connection->isExpired())
        {
            const auto & connection_info = connection_infos[connections_to_info_map[connection_idx]];
            connection = getConnection(connection_info);
        }
    }
    return connections;
}
