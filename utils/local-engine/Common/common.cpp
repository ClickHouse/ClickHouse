#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <jni.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <filesystem>
#include <Storages/HDFS/HDFSCommon.h>

using namespace DB;
#ifdef __cplusplus
extern "C" {
#endif

void registerAllFunctions()
{
    registerFunctions();
    registerAggregateFunctions();
}

/// If spark need to access hdfs, the environment variable `HADOOP_CONF_DIR` should have been set.
/// For clickhosue, environment variable `LIBHDFS3_CONF` need to be set to initialize libhdfs3.
/// So we use`HADOOP_CONF_DIR` from spark to setup `LIBHDFS3_CONF`.
///
/// TODO : Maybe initialize libhdfs3 by configure map but not a configure file path. Need to modify
/// clickhouse.
void setupHDFSConf(DB::Context::ConfigurationPtr context_config)
{
    const char * env_var = getenv("HADOOP_CONF_DIR");
    if (env_var)
    {
        std::filesystem::path conf_dir(env_var);
        auto conf_path = conf_dir / "hdfs-site.xml";
        if (!std::filesystem::exists(conf_path))
        {
            LOG_WARNING(&Poco::Logger::get("local_engine"), "Not found hdfs configure file:{}", conf_path.string());
        }
        else
        {
            LOG_INFO(&Poco::Logger::get("local_engine"), "Settup hdfs.libhdfs3_conf by {}", conf_path.string());
            context_config->setString("hdfs.libhdfs3_conf1", conf_path.string());
        }
    }
    else
    {
        LOG_INFO(&Poco::Logger::get("local_engine"), "HADOOP_CONF_DIR is not setted, hdfs access may fail");
    }

}
void init()
{
    static std::once_flag init_flag;
    std::call_once(
        init_flag,
        []()
        {
            registerAllFunctions();
            local_engine::Logger::initConsoleLogger();
#if USE_EMBEDDED_COMPILER
            /// 128 MB
            constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
            constexpr size_t compiled_expression_cache_elements_size_default = 10000;
            CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size_default, compiled_expression_cache_size_default);
#endif
        });

    static std::mutex context_lock;

    {
        std::lock_guard lock(context_lock);
        if (!local_engine::SerializedPlanParser::global_context)
        {
            local_engine::SerializedPlanParser::shared_context = SharedContextHolder(Context::createShared());
            local_engine::SerializedPlanParser::global_context
                = Context::createGlobal(local_engine::SerializedPlanParser::shared_context.get());
            local_engine::SerializedPlanParser::global_context->makeGlobalContext();
            local_engine::SerializedPlanParser::global_context->setSetting("join_use_nulls", true);
            setupHDFSConf(local_engine::SerializedPlanParser::config);
            local_engine::SerializedPlanParser::global_context->setConfig(local_engine::SerializedPlanParser::config);
            local_engine::SerializedPlanParser::global_context->setPath("/");
        }
    }
}

char * createExecutor(std::string plan_string)
{
    auto context = Context::createCopy(local_engine::SerializedPlanParser::global_context);
    local_engine::SerializedPlanParser parser(context);
    auto query_plan = parser.parse(plan_string);
    local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context);
    executor->execute(std::move(query_plan));
    return reinterpret_cast<char *>(executor);
}

bool executorHasNext(char * executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
}

#ifdef __cplusplus
}
#endif
