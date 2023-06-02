#include "Runner.h"
#include <Poco/Util/AbstractConfiguration.h>

#include "Common/ZooKeeper/ZooKeeperCommon.h"
#include "Common/ZooKeeper/ZooKeeperConstants.h"
#include <Common/EventNotifier.h>
#include <Common/Config/ConfigProcessor.h>
#include "IO/ReadBufferFromString.h"
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int BAD_ARGUMENTS;
}

Runner::Runner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const Strings & hosts_strings_,
        std::optional<double> max_time_,
        std::optional<double> delay_,
        std::optional<bool> continue_on_error_,
        std::optional<size_t> max_iterations_)
        : info(std::make_shared<Stats>())
{

    DB::ConfigProcessor config_processor(config_path, true, false);
    auto config = config_processor.loadConfig().configuration;

    generator.emplace(*config);

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

    std::cerr << "---- Run options ---- " << std::endl;
    static constexpr uint64_t DEFAULT_CONCURRENCY = 1;
    if (concurrency_)
        concurrency = *concurrency_;
    else
        concurrency = config->getUInt64("concurrency", DEFAULT_CONCURRENCY);
    std::cerr << "Concurrency: " << concurrency << std::endl;

    static constexpr uint64_t DEFAULT_ITERATIONS = 0;
    if (max_iterations_)
        max_iterations = *max_iterations_;
    else
        max_iterations = config->getUInt64("iterations", DEFAULT_ITERATIONS);
    std::cerr << "Iterations: " << max_iterations << std::endl;

    static constexpr double DEFAULT_DELAY = 1.0;
    if (delay_)
        delay = *delay_;
    else
        delay = config->getDouble("report_delay", DEFAULT_DELAY);
    std::cerr << "Report delay: " << delay << std::endl;

    static constexpr double DEFAULT_TIME_LIMIT = 0.0;
    if (max_time_)
        max_time = *max_time_;
    else
        max_time = config->getDouble("timelimit", DEFAULT_TIME_LIMIT);
    std::cerr << "Time limit: " << max_time << std::endl;

    if (continue_on_error_)
        continue_on_error = *continue_on_error_;
    else
        continue_on_error = config->getBool("continue_on_error", false);
    std::cerr << "Continue on error: " << continue_on_error << std::endl;

    static const std::string output_key = "output";
    print_to_stdout = config->getBool(output_key + ".stdout", false);
    std::cerr << "Printing output to stdout: " << print_to_stdout << std::endl;

    static const std::string output_file_key = output_key + ".file";
    if (config->has(output_file_key))
    {
        if (config->has(output_file_key + ".path"))
        {
            file_output = config->getString(output_file_key + ".path");
            output_file_with_timestamp = config->getBool(output_file_key + ".with_timestamp");
        }
        else
            file_output = config->getString(output_file_key);

        std::cerr << "Result file path: " << file_output->string() << std::endl;
    }

    std::cerr << "---- Run options ----\n" << std::endl;

    pool.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, concurrency);
    queue.emplace(concurrency);
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
            extracted = queue->tryPop(request, 100);

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
        Coordination::ResponseCallback callback = [&request, promise](const Coordination::Response & response)
        {
            bool set_exception = true;

            if (response.error == Coordination::Error::ZOK)
            {
                set_exception = false;
            }
            else if (response.error == Coordination::Error::ZNONODE)
            {
                /// remove can fail with ZNONODE because of different order of execution
                /// of generated create and remove requests
                /// this is okay for concurrent runs
                if (dynamic_cast<const Coordination::ZooKeeperRemoveResponse *>(&response))
                    set_exception = false;
                else if (const auto * multi_response = dynamic_cast<const Coordination::ZooKeeperMultiResponse *>(&response))
                {
                    const auto & responses = multi_response->responses;
                    size_t i = 0;
                    while (responses[i]->error != Coordination::Error::ZNONODE)
                        ++i;

                    const auto & multi_request = dynamic_cast<const Coordination::ZooKeeperMultiRequest &>(*request);
                    if (dynamic_cast<const Coordination::ZooKeeperRemoveRequest *>(&*multi_request.requests[i]))
                        set_exception = false;
                }
            }

            if (set_exception)
                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
            else
                promise->set_value(response.bytesSize());
        };

        Stopwatch watch;

        zk->executeGenericRequest(request, callback);

        try
        {
            auto response_size = future.get();
            auto microseconds = watch.elapsedMicroseconds();

            std::lock_guard lock(mutex);

            if (request->isReadRequest())
                info->addRead(microseconds, 1, request->bytesSize() + response_size);
            else
                info->addWrite(microseconds, 1, request->bytesSize() + response_size);
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

bool Runner::tryPushRequestInteractively(Coordination::ZooKeeperRequestPtr && request, DB::InterruptListener & interrupt_listener)
{
    bool inserted = false;

    while (!inserted)
    {
        inserted = queue->tryPush(std::move(request), 100);

        if (shutdown)
        {
            /// An exception occurred in a worker
            return false;
        }

        if (max_time > 0 && total_watch.elapsedSeconds() >= max_time)
        {
            std::cerr << "Stopping launch of queries. Requested time limit is exhausted.\n";
            return false;
        }

        if (interrupt_listener.check())
        {
            std::cerr << "Stopping launch of queries. SIGINT received." << std::endl;
            return false;
        }

        if (delay > 0 && delay_watch.elapsedSeconds() > delay)
        {
            printNumberOfRequestsExecuted(requests_executed);

            std::lock_guard lock(mutex);
            info->report(concurrency);
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

    auto start_timestamp_ms = Poco::Timestamp().epochMicroseconds() / 1000;

    try
    {
        for (size_t i = 0; i < concurrency; ++i)
        {
            auto thread_connections = connections;
            pool->scheduleOrThrowOnError([this, connections = std::move(thread_connections)]() mutable { thread(connections); });
        }
    }
    catch (...)
    {
        shutdown = true;
        pool->wait();
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

    pool->wait();
    total_watch.stop();

    printNumberOfRequestsExecuted(requests_executed);

    std::lock_guard lock(mutex);
    info->report(concurrency);

    DB::WriteBufferFromOwnString out;
    info->writeJSON(out, concurrency, start_timestamp_ms);
    auto output_string = std::move(out.str());

    if (print_to_stdout)
        std::cout << output_string << std::endl;

    if (file_output)
    {
        auto path = *file_output;

        if (output_file_with_timestamp)
        {
            auto filename = file_output->filename();
            filename = fmt::format("{}_{}{}", filename.stem().generic_string(), start_timestamp_ms, filename.extension().generic_string());
            path = file_output->parent_path() / filename;
        }

        std::cerr << "Storing output to " << path << std::endl;

        DB::WriteBufferFromFile file_output_buffer(path);
        DB::ReadBufferFromString read_buffer(output_string);
        DB::copyData(read_buffer, file_output_buffer);
    }
}


void Runner::createConnections()
{
    DB::EventNotifier::init();
    std::cerr << "---- Creating connections ---- " << std::endl;
    for (size_t connection_info_idx = 0; connection_info_idx < connection_infos.size(); ++connection_info_idx)
    {
        const auto & connection_info = connection_infos[connection_info_idx];
        std::cerr << fmt::format("Creating {} session(s) for:\n"
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
    std::cerr << "---- Done creating connections ----\n" << std::endl;
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

Runner::~Runner()
{
    queue->clearAndFinish();
    shutdown = true;
    pool->wait();
    generator->cleanup(*connections[0]);
}

