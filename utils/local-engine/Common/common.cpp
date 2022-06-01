#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Common/Logger.h>
#include <jni.h>

using namespace DB;
#ifdef __cplusplus
extern "C" {
#endif

void registerAllFunctions()
{
    registerFunctions();
    registerAggregateFunctions();
}

void init()
{
    registerAllFunctions();
    local_engine::SerializedPlanParser::shared_context = SharedContextHolder(Context::createShared());
    local_engine::SerializedPlanParser::global_context = Context::createGlobal(local_engine::SerializedPlanParser::shared_context.get());
    // disable global context initialized
    local_engine::SerializedPlanParser::global_context->setBackgroundExecutorsInitialized(true);
    local_engine::SerializedPlanParser::global_context->makeGlobalContext();
    local_engine::SerializedPlanParser::global_context->setSetting("join_use_nulls", true);
    local_engine::SerializedPlanParser::global_context->setConfig(local_engine::SerializedPlanParser::config);
    local_engine::SerializedPlanParser::global_context->setPath("/");
    local_engine::Logger::initConsoleLogger();

    /// 128 MB
    constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
    constexpr size_t compiled_expression_cache_elements_size_default = 10000;
    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size_default, compiled_expression_cache_size_default);
}

char * createExecutor(std::string plan_string)
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
