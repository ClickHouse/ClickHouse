#include "Runner.h"
#include <atomic>
#include <Poco/Util/AbstractConfiguration.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStorage.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Disks/DiskLocal.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/registerFormats.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/EventNotifier.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::Setting
{
    extern const SettingsUInt64 max_block_size;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

Runner::Runner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const std::string & input_request_log_,
        const std::string & setup_nodes_snapshot_path_,
        const Strings & hosts_strings_,
        std::optional<double> max_time_,
        std::optional<double> delay_,
        std::optional<bool> continue_on_error_,
        std::optional<size_t> max_iterations_)
        : input_request_log(input_request_log_)
        , setup_nodes_snapshot_path(setup_nodes_snapshot_path_)
        , info(std::make_shared<Stats>())
{

    DB::ConfigProcessor config_processor(config_path, true, false);
    DB::ConfigurationPtr config = nullptr;

    if (!config_path.empty())
    {
        config = config_processor.loadConfig().configuration;

        if (config->has("generator"))
            generator.emplace(*config);
    }
    else
    {
        if (input_request_log.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Both --config and --input_request_log cannot be empty");

        if (!std::filesystem::exists(input_request_log))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "File on path {} does not exist", input_request_log);
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

    std::cerr << "---- Run options ---- " << std::endl;
    static constexpr uint64_t DEFAULT_CONCURRENCY = 1;
    if (concurrency_)
        concurrency = *concurrency_;
    else if (!config)
        concurrency = DEFAULT_CONCURRENCY;
    else
        concurrency = config->getUInt64("concurrency", DEFAULT_CONCURRENCY);
    std::cerr << "Concurrency: " << concurrency << std::endl;

    static constexpr uint64_t DEFAULT_ITERATIONS = 0;
    if (max_iterations_)
        max_iterations = *max_iterations_;
    else if (!config)
        max_iterations = DEFAULT_ITERATIONS;
    else
        max_iterations = config->getUInt64("iterations", DEFAULT_ITERATIONS);
    std::cerr << "Iterations: " << max_iterations << std::endl;

    static constexpr double DEFAULT_DELAY = 1.0;
    if (delay_)
        delay = *delay_;
    else if (!config)
        delay = DEFAULT_DELAY;
    else
        delay = config->getDouble("report_delay", DEFAULT_DELAY);
    std::cerr << "Report delay: " << delay << std::endl;

    static constexpr double DEFAULT_TIME_LIMIT = 0.0;
    if (max_time_)
        max_time = *max_time_;
    else if (!config)
        max_time = DEFAULT_TIME_LIMIT;
    else
        max_time = config->getDouble("timelimit", DEFAULT_TIME_LIMIT);
    std::cerr << "Time limit: " << max_time << std::endl;

    if (continue_on_error_)
        continue_on_error = *continue_on_error_;
    else if (!config)
        continue_on_error_ = false;
    else
        continue_on_error = config->getBool("continue_on_error", false);
    std::cerr << "Continue on error: " << continue_on_error << std::endl;

    if (config)
    {
        benchmark_context.initializeFromConfig(*config);

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
    }

    std::cerr << "---- Run options ----\n" << std::endl;
}

void Runner::parseHostsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
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

        if (config.has(key + ".use_compression"))
            connection_info.use_compression = config.getBool(key + ".use_compression");
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
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_BLOCK_SIGNAL, "Cannot block signal");
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
    if (generator)
        runBenchmarkWithGenerator();
    else
        runBenchmarkFromLog();
}


struct ZooKeeperRequestBlock
{
    explicit ZooKeeperRequestBlock(DB::Block block_)
        : block(std::move(block_))
        , hostname_idx(block.getPositionByName("hostname"))
        , request_event_time_idx(block.getPositionByName("request_event_time"))
        , thread_id_idx(block.getPositionByName("thread_id"))
        , session_id_idx(block.getPositionByName("session_id"))
        , xid_idx(block.getPositionByName("xid"))
        , has_watch_idx(block.getPositionByName("has_watch"))
        , op_num_idx(block.getPositionByName("op_num"))
        , path_idx(block.getPositionByName("path"))
        , data_idx(block.getPositionByName("data"))
        , is_ephemeral_idx(block.getPositionByName("is_ephemeral"))
        , is_sequential_idx(block.getPositionByName("is_sequential"))
        , response_event_time_idx(block.getPositionByName("response_event_time"))
        , error_idx(block.getPositionByName("error"))
        , requests_size_idx(block.getPositionByName("requests_size"))
        , version_idx(block.getPositionByName("version"))
    {}

    size_t rows() const
    {
        return block.rows();
    }

    UInt64 getExecutorId(size_t row) const
    {
        return getSessionId(row);
    }

    std::string getHostname(size_t row) const
    {
        return getField(hostname_idx, row).safeGet<std::string>();
    }

    UInt64 getThreadId(size_t row) const
    {
        return getField(thread_id_idx, row).safeGet<UInt64>();
    }

    DB::DateTime64 getRequestEventTime(size_t row) const
    {
        return getField(request_event_time_idx, row).safeGet<DB::DateTime64>();
    }

    DB::DateTime64 getResponseEventTime(size_t row) const
    {
        return getField(response_event_time_idx, row).safeGet<DB::DateTime64>();
    }

    Int64 getSessionId(size_t row) const
    {
        return getField(session_id_idx, row).safeGet<Int64>();
    }

    Int64 getXid(size_t row) const
    {
        return getField(xid_idx, row).safeGet<Int64>();
    }

    bool hasWatch(size_t row) const
    {
        return getField(has_watch_idx, row).safeGet<UInt8>();
    }

    Coordination::OpNum getOpNum(size_t row) const
    {
        return static_cast<Coordination::OpNum>(getField(op_num_idx, row).safeGet<Int64>());
    }

    bool isEphemeral(size_t row) const
    {
        return getField(is_ephemeral_idx, row).safeGet<UInt8>();
    }

    bool isSequential(size_t row) const
    {
        return getField(is_sequential_idx, row).safeGet<UInt8>();
    }

    std::string getPath(size_t row) const
    {
        return getField(path_idx, row).safeGet<std::string>();
    }

    std::string getData(size_t row) const
    {
        return getField(data_idx, row).safeGet<std::string>();
    }

    UInt64 getRequestsSize(size_t row) const
    {
        return getField(requests_size_idx, row).safeGet<UInt64>();
    }

    std::optional<Int32> getVersion(size_t row) const
    {
        auto field = getField(version_idx, row);
        if (field.isNull())
            return std::nullopt;
        return static_cast<Int32>(field.safeGet<Int64>());
    }

    std::optional<Coordination::Error> getError(size_t row) const
    {
        auto field = getField(error_idx, row);
        if (field.isNull())
            return std::nullopt;

        return static_cast<Coordination::Error>(field.safeGet<Int64>());
    }
private:
    DB::Field getField(size_t position, size_t row) const
    {
        DB::Field field;
        block.getByPosition(position).column->get(row, field);
        return field;
    }

    DB::Block block;
    size_t hostname_idx = 0;
    size_t request_event_time_idx = 0;
    size_t thread_id_idx = 0;
    size_t session_id_idx = 0;
    size_t xid_idx = 0;
    size_t has_watch_idx = 0;
    size_t op_num_idx = 0;
    size_t path_idx = 0;
    size_t data_idx = 0;
    size_t is_ephemeral_idx = 0;
    size_t is_sequential_idx = 0;
    size_t response_event_time_idx = 0;
    size_t error_idx = 0;
    size_t requests_size_idx = 0;
    size_t version_idx = 0;
};

struct RequestFromLog
{
    Coordination::ZooKeeperRequestPtr request;
    std::optional<Coordination::Error> expected_result;
    std::vector<std::optional<Coordination::Error>> subrequest_expected_results;
    int64_t session_id = 0;
    size_t executor_id = 0;
    bool has_watch = false;
    DB::DateTime64 request_event_time;
    DB::DateTime64 response_event_time;
    std::shared_ptr<Coordination::ZooKeeper> connection;
};

struct ZooKeeperRequestFromLogReader
{
    ZooKeeperRequestFromLogReader(const std::string & input_request_log, DB::ContextPtr context)
    {
        std::optional<DB::FormatSettings> format_settings;

        file_read_buf = std::make_unique<DB::ReadBufferFromFile>(input_request_log);
        auto compression_method = DB::chooseCompressionMethod(input_request_log, "");
        file_read_buf = DB::wrapReadBufferWithCompressionMethod(std::move(file_read_buf), compression_method);

        DB::SingleReadBufferIterator read_buffer_iterator(std::move(file_read_buf));
        auto [columns_description, format] = DB::detectFormatAndReadSchema(format_settings, read_buffer_iterator, context);

        DB::ColumnsWithTypeAndName columns;
        columns.reserve(columns_description.size());

        for (const auto & column_description : columns_description)
            columns.push_back(DB::ColumnWithTypeAndName{column_description.type, column_description.name});

        header_block = std::move(columns);

        file_read_buf
            = DB::wrapReadBufferWithCompressionMethod(std::make_unique<DB::ReadBufferFromFile>(input_request_log), compression_method);

        input_format = DB::FormatFactory::instance().getInput(
            format,
            *file_read_buf,
            header_block,
            context,
            context->getSettingsRef()[DB::Setting::max_block_size],
            format_settings,
            1,
            std::nullopt,
            /*is_remote_fs*/ false,
            DB::CompressionMethod::None,
            false);

        Coordination::ACL acl;
        acl.permissions = Coordination::ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        default_acls.emplace_back(std::move(acl));
    }

    std::optional<RequestFromLog> getNextRequest(bool for_multi = false)
    {
        RequestFromLog request_from_log;

        if (!current_block)
        {
            auto chunk = input_format->generate();

            if (chunk.empty())
                return std::nullopt;

            current_block.emplace(header_block.cloneWithColumns(chunk.detachColumns()));
            idx_in_block = 0;
        }

        request_from_log.expected_result = current_block->getError(idx_in_block);
        request_from_log.session_id = current_block->getSessionId(idx_in_block);
        request_from_log.has_watch = current_block->hasWatch(idx_in_block);
        request_from_log.executor_id = current_block->getExecutorId(idx_in_block);
        request_from_log.request_event_time = current_block->getRequestEventTime(idx_in_block);
        request_from_log.response_event_time = current_block->getResponseEventTime(idx_in_block);

        const auto move_row_iterator = [&]
        {
            if (idx_in_block == current_block->rows() - 1)
                current_block.reset();
            else
                ++idx_in_block;
        };

        auto op_num = current_block->getOpNum(idx_in_block);
        switch (op_num)
        {
            case Coordination::OpNum::Create:
            {
                auto create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
                create_request->path = current_block->getPath(idx_in_block);
                create_request->data = current_block->getData(idx_in_block);
                create_request->is_ephemeral = current_block->isEphemeral(idx_in_block);
                create_request->is_sequential = current_block->isSequential(idx_in_block);
                request_from_log.request = create_request;
                break;
            }
            case Coordination::OpNum::Set:
            {
                auto set_request = std::make_shared<Coordination::ZooKeeperSetRequest>();
                set_request->path = current_block->getPath(idx_in_block);
                set_request->data = current_block->getData(idx_in_block);
                if (auto version = current_block->getVersion(idx_in_block))
                {
                    /// we just need to make sure that the request with version that need to fail, fail when replaying
                    if (request_from_log.expected_result == Coordination::Error::ZBADVERSION)
                        set_request->version = std::numeric_limits<int32_t>::max();
                }
                request_from_log.request = set_request;
                break;
            }
            case Coordination::OpNum::Remove:
            {
                auto remove_request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
                remove_request->path = current_block->getPath(idx_in_block);
                if (auto version = current_block->getVersion(idx_in_block))
                {
                    /// we just need to make sure that the request with version that need to fail, fail when replaying
                    if (request_from_log.expected_result == Coordination::Error::ZBADVERSION)
                        remove_request->version = std::numeric_limits<int32_t>::max();
                }
                request_from_log.request = remove_request;
                break;
            }
            case Coordination::OpNum::Check:
            case Coordination::OpNum::CheckNotExists:
            {
                auto check_request = std::make_shared<Coordination::ZooKeeperCheckRequest>();
                check_request->path = current_block->getPath(idx_in_block);
                if (auto version = current_block->getVersion(idx_in_block))
                {
                    /// we just need to make sure that the request with version that need to fail, fail when replaying
                    if (request_from_log.expected_result == Coordination::Error::ZBADVERSION)
                        check_request->version = std::numeric_limits<int32_t>::max();
                }
                if (op_num == Coordination::OpNum::CheckNotExists)
                    check_request->not_exists = true;
                request_from_log.request = check_request;
                break;
            }
            case Coordination::OpNum::Sync:
            {
                auto sync_request = std::make_shared<Coordination::ZooKeeperSyncRequest>();
                sync_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = sync_request;
                break;
            }
            case Coordination::OpNum::Get:
            {
                auto get_request = std::make_shared<Coordination::ZooKeeperGetRequest>();
                get_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = get_request;
                break;
            }
            case Coordination::OpNum::SimpleList:
            case Coordination::OpNum::FilteredList:
            {
                auto list_request = std::make_shared<Coordination::ZooKeeperSimpleListRequest>();
                list_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = list_request;
                break;
            }
            case Coordination::OpNum::Exists:
            {
                auto exists_request = std::make_shared<Coordination::ZooKeeperExistsRequest>();
                exists_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = exists_request;
                break;
            }
            case Coordination::OpNum::Multi:
            case Coordination::OpNum::MultiRead:
            {
                if (for_multi)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Nested multi requests are not allowed");

                auto requests_size = current_block->getRequestsSize(idx_in_block);

                Coordination::Requests requests;
                requests.reserve(requests_size);
                move_row_iterator();

                for (size_t i = 0; i < requests_size; ++i)
                {
                    auto subrequest_from_log = getNextRequest(/*for_multi=*/true);
                    if (!subrequest_from_log)
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to fetch subrequest for {}, subrequest index {}", op_num, i);

                    if (!subrequest_from_log->expected_result && request_from_log.expected_result
                        && request_from_log.expected_result == Coordination::Error::ZOK)
                    {
                        subrequest_from_log->expected_result = Coordination::Error::ZOK;
                    }

                    requests.push_back(std::move(subrequest_from_log->request));

                    if (subrequest_from_log->session_id != request_from_log.session_id)
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Session id mismatch for subrequest in {}, subrequest index {}", op_num, i);

                    if (subrequest_from_log->executor_id != request_from_log.executor_id)
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Executor id mismatch for subrequest in {}, subrequest index {}", op_num, i);

                    request_from_log.subrequest_expected_results.push_back(subrequest_from_log->expected_result);
                }

                request_from_log.request = std::make_shared<Coordination::ZooKeeperMultiRequest>(requests, default_acls);

                return request_from_log;
            }
            default:
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported operation {} ({})", op_num, static_cast<int64_t>(op_num));
        }

        move_row_iterator();

        return request_from_log;
    }

private:
    DB::Block header_block;

    std::unique_ptr<DB::ReadBuffer> file_read_buf;
    DB::InputFormatPtr input_format;

    std::optional<ZooKeeperRequestBlock> current_block;
    size_t idx_in_block = 0;

    Coordination::ACLs default_acls;
};


namespace
{

struct RequestFromLogStats
{
    struct Stats
    {
        std::atomic<size_t> total = 0;
        std::atomic<size_t> unexpected_results = 0;
    };

    Stats write_requests;
    Stats read_requests;
};

struct SetupNodeCollector
{
    explicit SetupNodeCollector(const std::string & setup_nodes_snapshot_path)
    {
        if (setup_nodes_snapshot_path.empty())
            return;

        keeper_context = std::make_shared<DB::KeeperContext>(true, std::make_shared<Coordination::CoordinationSettings>());
        keeper_context->setDigestEnabled(true);
        keeper_context->setSnapshotDisk(
            std::make_shared<DB::DiskLocal>("Keeper-snapshots", setup_nodes_snapshot_path));

        snapshot_manager.emplace(1, keeper_context);
        auto snapshot_result = snapshot_manager->restoreFromLatestSnapshot();
        if (snapshot_result.storage == nullptr)
        {
            std::cerr << "No initial snapshot found" << std::endl;
            initial_storage = std::make_unique<Coordination::KeeperMemoryStorage>(
                /* tick_time_ms */ 500, /* superdigest */ "", keeper_context, /* initialize_system_nodes */ false);
            initial_storage->initializeSystemNodes();
        }
        else
        {
            std::cerr << "Loaded initial nodes from snapshot" << std::endl;
            initial_storage = std::move(snapshot_result.storage);
        }
    }

    void processRequest(const RequestFromLog & request_from_log)
    {
        if (!request_from_log.expected_result.has_value())
            return;


        auto process_request = [&](const Coordination::ZooKeeperRequest & request, const auto expected_result)
        {
            const auto & path = request.getPath();

            if (nodes_created_during_replay.contains(path))
                return;

            auto op_num = request.getOpNum();

            if (op_num == Coordination::OpNum::Create)
            {
                if (expected_result == Coordination::Error::ZNODEEXISTS)
                {
                    addExpectedNode(path);
                }
                else if (expected_result == Coordination::Error::ZOK)
                {
                    nodes_created_during_replay.insert(path);
                    /// we need to make sure ancestors exist
                    auto position = path.find_last_of('/');
                    if (position != 0)
                    {
                        auto parent_path = path.substr(0, position);
                        addExpectedNode(parent_path);
                    }
                }
            }
            else if (op_num == Coordination::OpNum::Remove)
            {
                if (expected_result == Coordination::Error::ZOK || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (op_num == Coordination::OpNum::Set)
            {
                if (expected_result == Coordination::Error::ZOK || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (op_num == Coordination::OpNum::Check)
            {
                if (expected_result == Coordination::Error::ZOK || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (op_num == Coordination::OpNum::CheckNotExists)
            {
                if (expected_result == Coordination::Error::ZNODEEXISTS || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (request.isReadRequest())
            {
                if (expected_result == Coordination::Error::ZOK)
                    addExpectedNode(path);
            }
        };

        const auto & request = request_from_log.request;
        if (request->getOpNum() == Coordination::OpNum::Multi || request->getOpNum() == Coordination::OpNum::MultiRead)
        {
            const auto & multi_request = dynamic_cast<const Coordination::ZooKeeperMultiRequest &>(*request);
            const auto & subrequests = multi_request.requests;

            for (size_t i = 0; i < subrequests.size(); ++i)
            {
                const auto & zookeeper_request = dynamic_cast<const Coordination::ZooKeeperRequest &>(*subrequests[i]);
                const auto subrequest_expected_result = request_from_log.subrequest_expected_results[i];
                if (subrequest_expected_result.has_value())
                    process_request(zookeeper_request, *subrequest_expected_result);

            }
        }
        else
            process_request(*request, *request_from_log.expected_result);
    }

    void addExpectedNode(const std::string & path)
    {
        std::lock_guard lock(nodes_mutex);

        if (initial_storage->container.contains(path))
            return;

        new_nodes = true;
        std::cerr << "Adding expected node " << path << std::endl;

        Coordination::Requests create_ops;

        size_t pos = 1;
        while (true)
        {
            pos = path.find('/', pos);
            if (pos == std::string::npos)
                break;

            auto request = zkutil::makeCreateRequest(path.substr(0, pos), "", zkutil::CreateMode::Persistent, true);
            create_ops.emplace_back(request);
            ++pos;
        }

        auto request = zkutil::makeCreateRequest(path, "", zkutil::CreateMode::Persistent, true);
        create_ops.emplace_back(request);

        auto next_zxid = initial_storage->getNextZXID();

        static Coordination::ACLs default_acls = []
        {
            Coordination::ACL acl;
            acl.permissions = Coordination::ACL::All;
            acl.scheme = "world";
            acl.id = "anyone";
            return Coordination::ACLs{std::move(acl)};
        }();

        auto multi_create_request = std::make_shared<Coordination::ZooKeeperMultiRequest>(create_ops, default_acls);
        initial_storage->preprocessRequest(multi_create_request, 1, 0, next_zxid, /* check_acl = */ false);
        auto responses = initial_storage->processRequest(multi_create_request, 1, next_zxid, /* check_acl = */ false);
        if (responses.size() > 1 || responses[0].response->error != Coordination::Error::ZOK)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid response after trying to create a node {}", responses[0].response->error);
    }

    void generateSnapshot()
    {
        std::lock_guard lock(nodes_mutex);
        if (!new_nodes)
        {
            std::cerr << "No new nodes added" << std::endl;
            return;
        }

        std::cerr << "Generating snapshot with starting data" << std::endl;
        DB::SnapshotMetadataPtr snapshot_meta = std::make_shared<DB::SnapshotMetadata>(initial_storage->getZXID(), 1, std::make_shared<nuraft::cluster_config>());
        DB::KeeperStorageSnapshot<Coordination::KeeperMemoryStorage> snapshot(initial_storage.get(), snapshot_meta);
        snapshot_manager->serializeSnapshotToDisk(snapshot);

        new_nodes = false;
    }

    std::mutex nodes_mutex;
    DB::KeeperContextPtr keeper_context;
    std::shared_ptr<Coordination::KeeperMemoryStorage> initial_storage;
    std::unordered_set<std::string> nodes_created_during_replay;
    std::optional<Coordination::KeeperSnapshotManager<Coordination::KeeperMemoryStorage>> snapshot_manager;
    bool new_nodes = false;
};

void dumpStats(std::string_view type, const RequestFromLogStats::Stats & stats_for_type)
{
    std::cerr << fmt::format(
        "{} requests: {} total, {} with unexpected results ({:.4}%)",
        type,
        stats_for_type.total,
        stats_for_type.unexpected_results,
        stats_for_type.total != 0 ? static_cast<double>(stats_for_type.unexpected_results) / stats_for_type.total * 100 : 0.0)
              << std::endl;
};

void requestFromLogExecutor(std::shared_ptr<ConcurrentBoundedQueue<RequestFromLog>> queue, RequestFromLogStats & request_stats)
{
    RequestFromLog request_from_log;
    std::optional<std::future<void>> last_request;
    while (queue->pop(request_from_log))
    {
        auto request_promise = std::make_shared<std::promise<void>>();
        last_request = request_promise->get_future();
        Coordination::ResponseCallback callback = [&,
                                                   request_promise,
                                                   request = request_from_log.request,
                                                   expected_result = request_from_log.expected_result,
                                                   subrequest_expected_results = std::move(request_from_log.subrequest_expected_results)](
                                                      const Coordination::Response & response) mutable
        {
            auto & stats = request->isReadRequest() ? request_stats.read_requests : request_stats.write_requests;

            stats.total.fetch_add(1, std::memory_order_relaxed);

            if (expected_result)
            {
                if (*expected_result != response.error)
                    stats.unexpected_results.fetch_add(1, std::memory_order_relaxed);

#if 0
                if (*expected_result != response.error)
                {
                    std::cerr << fmt::format(
                        "Unexpected result for {}\ngot {}, expected {}\n", request->toString(), response.error, *expected_result)
                              << std::endl;

                    if (const auto * multi_response = dynamic_cast<const Coordination::ZooKeeperMultiResponse *>(&response))
                    {
                        std::string subresponses;
                        for (size_t i = 0; i < multi_response->responses.size(); ++i)
                        {
                            subresponses += fmt::format("{} = {}\n", i, multi_response->responses[i]->error);
                        }

                        std::cerr << "Subresponses\n" << subresponses << std::endl;
                    }
                }
#endif
            }

            request_promise->set_value();
        };

        Coordination::WatchCallbackPtr watch;
        if (request_from_log.has_watch)
            watch = std::make_shared<Coordination::WatchCallback>([](const Coordination::WatchResponse &) {});

        request_from_log.connection->executeGenericRequest(request_from_log.request, callback, watch);
    }

    if (last_request)
        last_request->wait();
}

}

void Runner::runBenchmarkFromLog()
{
    std::cerr << fmt::format("Running benchmark using requests from {}", input_request_log) << std::endl;

    pool.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, concurrency);

    shared_context = DB::Context::createShared();
    global_context = DB::Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    DB::registerFormats();

    /// Randomly choosing connection index
    pcg64 rng(randomSeed());
    std::uniform_int_distribution<size_t> connection_distribution(0, connection_infos.size() - 1);

    std::unordered_map<int64_t, std::shared_ptr<Coordination::ZooKeeper>> zookeeper_connections;
    auto get_zookeeper_connection = [&](int64_t session_id)
    {
        if (auto it = zookeeper_connections.find(session_id); it != zookeeper_connections.end() && !it->second->isExpired())
            return it->second;

        auto connection_idx = connection_distribution(rng);
        auto zk_connection = getConnection(connection_infos[connection_idx], connection_idx);
        zookeeper_connections.insert_or_assign(session_id, zk_connection);
        return zk_connection;
    };

    RequestFromLogStats stats;

    std::optional<SetupNodeCollector> setup_nodes_collector;
    if (!setup_nodes_snapshot_path.empty())
        setup_nodes_collector.emplace(setup_nodes_snapshot_path);

    std::unordered_map<uint64_t, std::shared_ptr<ConcurrentBoundedQueue<RequestFromLog>>> executor_id_to_queue;

    SCOPE_EXIT_SAFE({
        for (const auto & [executor_id, executor_queue] : executor_id_to_queue)
            executor_queue->finish();

        pool->wait();


        if (setup_nodes_collector)
        {
            setup_nodes_collector->generateSnapshot();
        }
        else
        {
            dumpStats("Write", stats.write_requests);
            dumpStats("Read", stats.read_requests);
        }
    });

    auto push_request = [&](RequestFromLog request)
    {
        if (auto it = executor_id_to_queue.find(request.executor_id); it != executor_id_to_queue.end())
        {
            auto success = it->second->push(std::move(request));
            if (!success)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to push to the executor's queue");
            return;
        }

        auto executor_queue = std::make_shared<ConcurrentBoundedQueue<RequestFromLog>>(std::numeric_limits<uint64_t>::max());
        executor_id_to_queue.emplace(request.executor_id, executor_queue);
        auto scheduled = pool->trySchedule([&, executor_queue]() mutable
        {
            requestFromLogExecutor(std::move(executor_queue), stats);
        });

        if (!scheduled)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to schedule worker, try to increase concurrency parameter");

        auto success = executor_queue->push(std::move(request));
        if (!success)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to push to the executor's queue");
    };

    if (!setup_nodes_collector)
    {
        auto setup_connection = getConnection(connection_infos[0], 0);
        benchmark_context.startup(*setup_connection);
    }

    ZooKeeperRequestFromLogReader request_reader(input_request_log, global_context);

    delay_watch.restart();
    while (auto request_from_log = request_reader.getNextRequest())
    {
        if (setup_nodes_collector)
        {
            setup_nodes_collector->processRequest(*request_from_log);
        }
        else
        {
            request_from_log->connection = get_zookeeper_connection(request_from_log->session_id);
            request_from_log->executor_id %= concurrency;
            push_request(std::move(*request_from_log));
        }

        if (delay > 0 && delay_watch.elapsedSeconds() > delay)
        {
            if (setup_nodes_collector)
                setup_nodes_collector->generateSnapshot();
            else
            {
                dumpStats("Write", stats.write_requests);
                dumpStats("Read", stats.read_requests);
                std::cerr << std::endl;
            }
            delay_watch.restart();
        }
    }
}

void Runner::runBenchmarkWithGenerator()
{
    pool.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, concurrency);
    queue.emplace(concurrency);
    createConnections();

    std::cerr << "Preparing to run\n";
    benchmark_context.startup(*connections[0]);
    generator->startup(*connections[0]);
    std::cerr << "Prepared\n";

    auto start_timestamp_ms = Poco::Timestamp().epochMicroseconds() / 1000;

    try
    {
        for (size_t i = 0; i < concurrency; ++i)
        {
            auto thread_connections = connections;
            pool->scheduleOrThrowOnError([this, my_connections = std::move(thread_connections)]() mutable { thread(my_connections); });
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
            connections.emplace_back(getConnection(connection_info, connection_info_idx));
            connections_to_info_map[connections.size() - 1] = connection_info_idx;
        }
    }
    std::cerr << "---- Done creating connections ----\n" << std::endl;
}

std::shared_ptr<Coordination::ZooKeeper> Runner::getConnection(const ConnectionInfo & connection_info, size_t connection_info_idx)
{
    zkutil::ShuffleHost host;
    host.host = connection_info.host;
    host.secure = connection_info.secure;
    host.original_index = static_cast<UInt8>(connection_info_idx);
    host.address = Poco::Net::SocketAddress{connection_info.host};

    zkutil::ShuffleHosts nodes{host};
    zkutil::ZooKeeperArgs args;
    args.session_timeout_ms = connection_info.session_timeout_ms;
    args.connection_timeout_ms = connection_info.connection_timeout_ms;
    args.operation_timeout_ms = connection_info.operation_timeout_ms;
    args.use_compression = connection_info.use_compression;
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
            connection = getConnection(connection_info, connection_idx);
        }
    }
    return connections;
}

Runner::~Runner()
{
    if (queue)
        queue->clearAndFinish();
    shutdown = true;

    if (pool)
        pool->wait();

    try
    {
        auto connection = getConnection(connection_infos[0], 0);
        benchmark_context.cleanup(*connection);
    }
    catch (...)
    {
        DB::tryLogCurrentException("While trying to clean nodes");
    }
}

namespace
{

void removeRecursive(Coordination::ZooKeeper & zookeeper, const std::string & path)
{
    namespace fs = std::filesystem;

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    Strings children;
    auto list_callback = [promise, &children] (const Coordination::ListResponse & response)
    {
        children = response.names;
        promise->set_value();
    };
    zookeeper.list(path, Coordination::ListRequestType::ALL, list_callback, nullptr);
    future.get();

    std::span children_span(children);
    while (!children_span.empty())
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < 1000 && !children_span.empty(); ++i)
        {
            removeRecursive(zookeeper, fs::path(path) / children_span.back());
            ops.emplace_back(zkutil::makeRemoveRequest(fs::path(path) / children_span.back(), -1));
            children_span = children_span.subspan(0, children_span.size() - 1);
        }
        auto multi_promise = std::make_shared<std::promise<void>>();
        auto multi_future = multi_promise->get_future();

        auto multi_callback = [multi_promise] (const Coordination::MultiResponse &)
        {
            multi_promise->set_value();
        };
        zookeeper.multi(ops, multi_callback);
        multi_future.get();
    }
    auto remove_promise = std::make_shared<std::promise<void>>();
    auto remove_future = remove_promise->get_future();

    auto remove_callback = [remove_promise] (const Coordination::RemoveResponse &)
    {
        remove_promise->set_value();
    };

    zookeeper.remove(path, -1, remove_callback);
    remove_future.get();
}

}

void BenchmarkContext::initializeFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    std::cerr << "---- Parsing setup ---- " << std::endl;
    static const std::string setup_key = "setup";
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(setup_key, keys);
    for (const auto & key : keys)
    {
        if (key.starts_with("node"))
        {
            auto node_key = setup_key + "." + key;
            auto parsed_root_node = parseNode(node_key, config);
            const auto node = root_nodes.emplace_back(parsed_root_node);

            if (config.has(node_key + ".repeat"))
            {
                if (!node->name.isRandom())
                    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", node_key);

                auto repeat_count = config.getUInt64(node_key + ".repeat");
                node->repeat_count = repeat_count;
                for (size_t i = 1; i < repeat_count; ++i)
                    root_nodes.emplace_back(node->clone());
            }

            std::cerr << "Tree to create:" << std::endl;

            node->dumpTree();
            std::cerr << std::endl;
        }
    }
    std::cerr << "---- Done parsing data setup ----\n" << std::endl;
}

std::shared_ptr<BenchmarkContext::Node> BenchmarkContext::parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    auto node = std::make_shared<BenchmarkContext::Node>();
    node->name = StringGetter::fromConfig(key + ".name", config);

    if (config.has(key + ".data"))
        node->data = StringGetter::fromConfig(key + ".data", config);

    Poco::Util::AbstractConfiguration::Keys node_keys;
    config.keys(key, node_keys);

    for (const auto & node_key : node_keys)
    {
        if (!node_key.starts_with("node"))
            continue;

        const auto node_key_string = key + "." + node_key;
        auto child_node = parseNode(node_key_string, config);
        node->children.push_back(child_node);

        if (config.has(node_key_string + ".repeat"))
        {
            if (!child_node->name.isRandom())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", node_key_string);

            auto repeat_count = config.getUInt64(node_key_string + ".repeat");
            child_node->repeat_count = repeat_count;
            for (size_t i = 1; i < repeat_count; ++i)
                node->children.push_back(child_node);
        }
    }

    return node;
}

void BenchmarkContext::Node::dumpTree(int level) const
{
    std::string data_string
        = data.has_value() ? fmt::format("{}", data->description()) : "no data";

    std::string repeat_count_string = repeat_count != 0 ? fmt::format(", repeated {} times", repeat_count) : "";

    std::cerr << fmt::format("{}name: {}, data: {}{}", std::string(level, '\t'), name.description(), data_string, repeat_count_string) << std::endl;

    for (auto it = children.begin(); it != children.end();)
    {
        const auto & child = *it;
        child->dumpTree(level + 1);
        std::advance(it, child->repeat_count != 0 ? child->repeat_count : 1);
    }
}

std::shared_ptr<BenchmarkContext::Node> BenchmarkContext::Node::clone() const
{
    auto new_node = std::make_shared<Node>();
    new_node->name = name;
    new_node->data = data;
    new_node->repeat_count = repeat_count;

    // don't do deep copy of children because we will do clone only for root nodes
    new_node->children = children;

    return new_node;
}

void BenchmarkContext::Node::createNode(Coordination::ZooKeeper & zookeeper, const std::string & parent_path, const Coordination::ACLs & acls) const
{
    auto path = std::filesystem::path(parent_path) / name.getString();
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto create_callback = [promise] (const Coordination::CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        else
            promise->set_value();
    };
    zookeeper.create(path, data ? data->getString() : "", false, false, acls, create_callback);
    future.get();

    for (const auto & child : children)
        child->createNode(zookeeper, path, acls);
}

void BenchmarkContext::startup(Coordination::ZooKeeper & zookeeper)
{
    if (root_nodes.empty())
        return;

    std::cerr << "---- Creating test data ----" << std::endl;
    for (const auto & node : root_nodes)
    {
        auto node_name = node->name.getString();
        node->name.setString(node_name);

        std::string root_path = std::filesystem::path("/") / node_name;
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path);

        node->createNode(zookeeper, "/", default_acls);
    }
    std::cerr << "---- Created test data ----\n" << std::endl;
}

void BenchmarkContext::cleanup(Coordination::ZooKeeper & zookeeper)
{
    if (root_nodes.empty())
        return;

    std::cerr << "---- Cleaning up test data ----" << std::endl;
    for (const auto & node : root_nodes)
    {
        auto node_name = node->name.getString();
        std::string root_path = std::filesystem::path("/") / node_name;
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path);
    }
}
