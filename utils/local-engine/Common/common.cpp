#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/Logger.h>

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
    dbms::SerializedPlanParser::shared_context = SharedContextHolder(Context::createShared());
    dbms::SerializedPlanParser::global_context = Context::createGlobal(dbms::SerializedPlanParser::shared_context.get());
    dbms::SerializedPlanParser::global_context->makeGlobalContext();
    dbms::SerializedPlanParser::global_context->setConfig(dbms::SerializedPlanParser::config);
    dbms::SerializedPlanParser::global_context->setPath("/");
    local_engine::Logger::initConsoleLogger();
}

char * createExecutor(std::string plan_string)
{
    auto context = Context::createCopy(dbms::SerializedPlanParser::global_context);
    dbms::SerializedPlanParser parser(context);
    auto query_plan = parser.parse(plan_string);
    dbms::LocalExecutor * executor = new dbms::LocalExecutor(parser.query_context);
    executor->execute(std::move(query_plan));
    return reinterpret_cast<char* >(executor);
}

bool executorHasNext(char * executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    return executor->hasNext();
}

#ifdef __cplusplus
}
#endif
