#include "include/io_kyligence_jni_engine_LocalEngine.h"
#include <string>
#include <iostream>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>

#include <fstream>
#include <string>
#include <Processors/Pipe.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
//#include <Substrait/relations.pb.h>

using namespace DB;
using namespace rapidjson;

/**
 * SQL example：++
 *
 *     SELECT min(x1),max(x2),sum(x3),count(x4),avg(x5) FROM table1 WHERE x6=* GROUP BY x7
 *
 * table defination
 * SQL columns:
 *     project
 *     filter
 *     aggregate
 */
Block getTableHeader(std::map<std::string, std::string> & cols)
{
    auto internalCols = std::make_shared<std::vector<ColumnWithTypeAndName>>();
    internalCols->reserve(cols.size());
    for (const auto & [key, value] : cols)
    {
        ColumnWithTypeAndName col;
        auto & data_type_factory = DataTypeFactory::instance();
        auto type = data_type_factory.get(value);
        internalCols->push_back(ColumnWithTypeAndName(type->createColumn(), type, key));
    }
    return Block(*internalCols);
}

std::shared_ptr<CSVRowInputFormat> getSource(ReadBuffer & buf, Block & header)
{
    FormatSettings settings;
    return std::make_shared<CSVRowInputFormat>(header, buf, RowInputFormatParams{.max_block_size = 100}, false, settings);
}


std::shared_ptr<std::map<std::string, std::string>> getColumns(Document & config)
{
    auto columns = std::make_shared<std::map<std::string, std::string>>();
    auto cols = config["columns"].GetArray();
    for (auto * it = cols.Begin(); it != cols.End(); it++)
    {
        auto col = it->GetObject();
        if (columns->contains(col["name"].GetString()))
        {
            throw std::logic_error("duplicate column");
        }
        columns->emplace(col["name"].GetString(), col["type"].GetString());
    }
    return columns;
}

void registerAllFunctions()
{
    registerFunctions();
    registerAggregateFunctions();
}

FunctionOverloadResolverPtr getFunction(const std::string & name, ContextPtr context)
{
    auto & factory = FunctionFactory::instance();
    return factory.get(name, context);
}

AggregateFunctionPtr getAggregateFunction(const std::string & name, DataTypes arg_types)
{
    auto & factory = AggregateFunctionFactory::instance();
    AggregateFunctionProperties properties;
    return factory.get(name, arg_types, Array{}, properties);
}

ActionsDAG::NodeRawConstPtrs getArguments(ActionsDAG::NodeRawConstPtrs nodes, std::vector<std::string> & args)
{
    ActionsDAG::NodeRawConstPtrs result;
    result.reserve(args.size());
    for (const auto & item : nodes)
    {
        if (std::find(args.begin(), args.end(), item->result_name) != args.end())
        {
            result.emplace_back(item);
        }
    }
    return result;
}

NamesAndTypesList blockToNameAndTypeList(Block & header)
{
    NamesAndTypesList types;
    for (const auto & name : header.getNames())
    {
        auto column = header.findByName(name);
        types.push_back(NameAndTypePair(column->name, column->type));
    }
    return types;
}

QueryPlanStepPtr buildFilter(Block & header, ContextPtr context)
{
    auto actions_dag = std::make_shared<ActionsDAG>(std::move(blockToNameAndTypeList(header)));
    //    auto int_type = std::make_shared<DataTypeInt32>();
    //    auto const_node = actions_dag->addInput(ColumnWithTypeAndName(int_type->createColumnConst(1, 4), int_type, "_1"));
    //    actions_dag->addOrReplaceInIndex(const_node);
    std::string empty_string;
    std::vector<std::string> args = {"x1", "x2"};
    const auto & filter_node = actions_dag->addFunction(
        std::move(getFunction("less", context)), getArguments(actions_dag->getIndex(), args), std::move(empty_string));
    actions_dag->getIndex().push_back(&filter_node);
    DataStream input_stream = DataStream{.header = header};
    auto filter = std::make_unique<FilterStep>(input_stream, actions_dag, std::move(filter_node.result_name), true);
    return std::move(filter);
}

void buildAgg(Block & header, QueryPlan & query_plan, ContextPtr context)
{
    auto aggregates = AggregateDescriptions();
    auto count = AggregateDescription();
    count.column_name = "count(x2)";
    count.arguments = ColumnNumbers{1};
    count.argument_names = Names{"x2"};
    auto int_type = std::make_shared<DataTypeInt32>();
    count.function = getAggregateFunction("count", {int_type});
    aggregates.push_back(count);
    Settings settings;
    Aggregator::Params params(
        header,
        ColumnNumbers{0},
        aggregates,
        false,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        settings.group_by_two_level_threshold,
        settings.group_by_two_level_threshold_bytes,
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set,
        context->getTemporaryVolume(),
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        settings.compile_aggregate_expressions,
        settings.min_count_to_compile_aggregate_expression);

    SortDescription group_by_sort_description;

    auto merge_threads = 1;
    auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
        ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
        : static_cast<size_t>(settings.max_threads);


    auto aggregating_step = std::make_unique<AggregatingStep>(
        query_plan.getCurrentDataStream(),
        params,
        true,
        settings.max_block_size,
        merge_threads,
        temporary_data_merge_threads,
        false,
        nullptr,
        std::move(group_by_sort_description));

    query_plan.addStep(std::move(aggregating_step));
}

bool inside_main = false;

void runSamplePipeline()
{
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    registerAllFunctions();
    auto & factory = FunctionFactory::instance();
    std::ifstream ifs("/home/kyligence/Documents/code/ClickHouse/utils/local-engine/table.json");
    IStreamWrapper isw(ifs);

    Document d;
    d.ParseStream(isw);
    auto cols = getColumns(d);
    auto header = getTableHeader(*cols);

    QueryPlan query_plan;
    auto file = "/home/kyligence/Documents/code/ClickHouse/utils/local-engine/table.csv";
    auto buf = std::make_unique<ReadBufferFromFilePRead>(file);

    auto source = getSource(*buf, header);

    std::unique_ptr<QueryPipelines> query_pipelines = std::make_unique<QueryPipelines>();
    auto source_step = std::make_unique<ReadFromStorageStep>(Pipe(source), "CSV");
    query_plan.addStep(std::move(source_step));

    auto filter = buildFilter(header, global_context);
    query_plan.addStep(std::move(filter));
    buildAgg(header, query_plan, global_context);
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
    auto query_pipline = query_plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings());

    auto buffer = WriteBufferFromFile("/home/kyligence/Documents/code/ClickHouse/output.txt");
    auto output = std::make_shared<CSVRowOutputFormat>(buffer, query_pipline->getHeader(), true, RowOutputFormatParams(), FormatSettings());
    query_pipline->setOutputFormat(output);
    auto executor = query_pipline->execute();
    executor->execute(1);
}

int main(int, char **)
{
    inside_main = true;
    runSamplePipeline();
    return 0;
}

JNIEXPORT jlong JNICALL Java_io_kyligence_jni_engine_LocalEngine_test
    (JNIEnv *env, jclass, jint a, jint b)
{
    inside_main = true;
    std::cout << "start run pipeline." << std::endl;
    try
    {
        runSamplePipeline();
    }
    catch (Poco::Exception e)
    {
        std::cout << e.message() << std::endl;
        std::cout << e.displayText() << std::endl;
        e.rethrow();
    }
    std::cout << "run pipeline success." << std::endl;
    std::cout <<std::string("hello world");
    return a + b;
}