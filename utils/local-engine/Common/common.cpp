#include <AggregateFunctions/registerAggregateFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Logger.h>
#include <Poco/SimpleFileChannel.h>
#include <Poco/Util/MapConfiguration.h>
#include <jni.h>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}
using namespace DB;
namespace fs = std::filesystem;

namespace local_engine {
    extern void registerAggregateFunctionCombinatorPartialMerge(AggregateFunctionCombinatorFactory &);
}

#ifdef __cplusplus
extern "C" {
#endif

void registerAllFunctions()
{
    registerFunctions();

    registerAggregateFunctions();
    auto & factory = AggregateFunctionCombinatorFactory::instance();
    local_engine::registerAggregateFunctionCombinatorPartialMerge(factory);

}
constexpr auto CH_BACKEND_CONF_PREFIX = "spark.gluten.sql.columnar.backend.ch";
constexpr auto CH_RUNTIME_CONF = "runtime_conf";

/// For using gluten, we recommend to pass clickhouse runtime configure by using --files in spark-submit.
/// And set the parameter CH_BACKEND_CONF_PREFIX.CH_RUNTIME_CONF.conf_file
/// You can also set a specified configuration with prefix CH_BACKEND_CONF_PREFIX.CH_RUNTIME_CONF, and this
/// will overwrite the configuration from CH_BACKEND_CONF_PREFIX.CH_RUNTIME_CONF.conf_file .
static std::map<std::string, std::string> getBackendConf(const std::string & plan)
{
    std::map<std::string, std::string> ch_backend_conf;

    /// parse backend configs from plan extensions
    do
    {
        auto plan_ptr = std::make_unique<substrait::Plan>();
        auto success = plan_ptr->ParseFromString(plan);
        if (!success)
            break;

        if (!plan_ptr->has_advanced_extensions() || !plan_ptr->advanced_extensions().has_enhancement())
            break;
        const auto & enhancement = plan_ptr->advanced_extensions().enhancement();

        if (!enhancement.Is<substrait::Expression>())
            break;

        substrait::Expression expression;
        if (!enhancement.UnpackTo(&expression) || !expression.has_literal() || !expression.literal().has_map())
            break;

        const auto & key_values = expression.literal().map().key_values();
        for (const auto & key_value : key_values)
        {
             if (!key_value.has_key() || !key_value.has_value())
                continue;

            const auto & key = key_value.key();
            const auto & value = key_value.value();
            if (!key.has_string() || !value.has_string())
                continue;

            if (!key.string().starts_with(CH_BACKEND_CONF_PREFIX))
                continue;

            ch_backend_conf[key.string()] = value.string();
        }
    } while (false);

    std::string ch_runtime_conf_file = std::string(CH_BACKEND_CONF_PREFIX) + "." + std::string(CH_RUNTIME_CONF) + ".conf_file";
    if (!ch_backend_conf.count(ch_runtime_conf_file))
    {
        /// Try to get config path from environment variable
        const char * config_path = std::getenv("CLICKHOUSE_BACKEND_CONFIG");
        if (config_path)
        {
            ch_backend_conf[ch_runtime_conf_file] = config_path;
        }
    }
    return ch_backend_conf;
}

void initCHRuntimeConfig(const std::map<std::string, std::string> & conf)
{}

void init(const std::string & plan)
{
    static std::once_flag init_flag;
    std::call_once(
        init_flag,
        [&plan]()
        {
            /// Load Config
            std::map<std::string, std::string> ch_backend_conf;
            std::string ch_runtime_conf_prefix = std::string(CH_BACKEND_CONF_PREFIX) + "." + std::string(CH_RUNTIME_CONF);
            std::string ch_runtime_conf_file = ch_runtime_conf_prefix + ".conf_file";
            if (!local_engine::SerializedPlanParser::config)
            {
                ch_backend_conf = getBackendConf(plan);

                /// If we have a configuration file, use it at first
                if (ch_backend_conf.count(ch_runtime_conf_file))
                {
                    if (fs::exists(ch_runtime_conf_file) && fs::is_regular_file(ch_runtime_conf_file))
                    {
                        DB::ConfigProcessor config_processor(ch_runtime_conf_file, false, true);
                        config_processor.setConfigPath(fs::path(ch_runtime_conf_file).parent_path());
                        auto loaded_config = config_processor.loadConfig(false);
                        local_engine::SerializedPlanParser::config = loaded_config.configuration;
                    }
                    else
                    {
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} is not a valid configure file.", ch_runtime_conf_file);
                    }
                }
                else
                {
                    local_engine::SerializedPlanParser::config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
                }

                /// Update specified settings
                for (const auto & kv : ch_backend_conf)
                {
                    if (kv.first.starts_with(ch_runtime_conf_prefix) && kv.first != ch_runtime_conf_file)
                    {
                        /// Notice, you can set a conf by setString(), but get it by getInt()
                        local_engine::SerializedPlanParser::config->setString(
                            kv.first.substr(ch_runtime_conf_prefix.size() + 1), kv.second);
                    }
                }
            }

            /// Initialize Loggers
            auto & config = local_engine::SerializedPlanParser::config;
            auto level = config->getString("logger.level", "error");
            if (config->has("logger.log"))
            {
                local_engine::Logger::initFileLogger(*config, "ClickHouseBackend");
            }
            else
            {
                local_engine::Logger::initConsoleLogger(level);
            }
            LOG_INFO(&Poco::Logger::get("ClickHouseBackend"), "Init logger.");

            /// Initialize settings
            const std::string prefix = "local_engine.";
            auto settings = Settings();
            if (config->has(prefix + "settings"))
            {
                settings.loadSettingsFromConfig(prefix + "settings", *config);
            }
            settings.set("join_use_nulls", true);
            LOG_INFO(&Poco::Logger::get("ClickHouseBackend"), "Init settings.");

            /// Initialize global context
            if (!local_engine::SerializedPlanParser::global_context)
            {
                local_engine::SerializedPlanParser::shared_context = SharedContextHolder(Context::createShared());
                local_engine::SerializedPlanParser::global_context
                    = Context::createGlobal(local_engine::SerializedPlanParser::shared_context.get());
                local_engine::SerializedPlanParser::global_context->makeGlobalContext();
                local_engine::SerializedPlanParser::global_context->setConfig(config);
                local_engine::SerializedPlanParser::global_context->setSettings(settings);

                auto path = config->getString("path", "/");
                local_engine::SerializedPlanParser::global_context->setPath(path);
                LOG_INFO(&Poco::Logger::get("ClickHouseBackend"), "Init global context.");
            }

            registerAllFunctions();
            LOG_INFO(&Poco::Logger::get("ClickHouseBackend"), "Register all functions.");

#if USE_EMBEDDED_COMPILER
            /// 128 MB
            constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
            size_t compiled_expression_cache_size = config->getUInt64("compiled_expression_cache_size", compiled_expression_cache_size_default);

            constexpr size_t compiled_expression_cache_elements_size_default = 10000;
            size_t compiled_expression_cache_elements_size = config->getUInt64("compiled_expression_cache_elements_size", compiled_expression_cache_elements_size_default);

            CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size, compiled_expression_cache_elements_size);
            LOG_INFO(&Poco::Logger::get("ClickHouseBackend"), "Init compiled expressions cache factory.");
#endif
        }

    );
}

char * createExecutor(const std::string & plan_string)
{
    auto context = Context::createCopy(local_engine::SerializedPlanParser::global_context);
    local_engine::SerializedPlanParser parser(context);
    auto query_plan = parser.parse(plan_string);
    local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context);
    executor->execute(std::move(query_plan));
    return reinterpret_cast<char* >(executor);
}

bool executorHasNext(char * executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
}

#ifdef __cplusplus
}
#endif
