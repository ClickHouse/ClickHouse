#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/Context.h>
#include <Core/NamesAndTypes.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <IO/ReadBuffer.h>

#include <fstream>
#include <iostream>
#include <string>
#include <Processors/Pipe.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>


using namespace DB;
using namespace rapidjson;

/**
 * SQL example：
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

std::shared_ptr<CSVRowInputFormat> getSource(const std::string &file, Block &header) {
//    std::unique_ptr<ReadBufferFromFile> buf = std::make_unique<ReadBufferFromFile>(file);
    auto buf = ReadBufferFromFile(file);
    FormatSettings settings;
    return std::make_shared<CSVRowInputFormat>(header, buf, RowInputFormatParams{.max_block_size=100}, false, settings);
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

FunctionOverloadResolverPtr getFunction(const std::string & name)
{
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    auto & factory = FunctionFactory::instance();
    return factory.get(name, global_context);
}

ActionsDAG::NodeRawConstPtrs getArguments(ActionsDAG::NodeRawConstPtrs nodes, std::vector<std::string>& args) {
    ActionsDAG::NodeRawConstPtrs result;
    result.reserve(args.size());
    for (const auto &item : nodes)
    {
        if (std::find(args.begin(), args.end(), item->result_name) != args.end()) {
            result.emplace_back(item);
        }
    }
    return result;
}

NamesAndTypesList blockToNameAndTypeList(Block & header)
{
    NamesAndTypesList types;
    for (const auto &name : header.getNames())
    {
        auto column = header.findByName(name);
        types.push_back(NameAndTypePair(column->name, column->type));
    }
    return types;
}

QueryPlanStepPtr buildFilter(Block & header)
{
    auto actions_dag = std::make_shared<ActionsDAG>(std::move(blockToNameAndTypeList(header)));
//    auto int_type = std::make_shared<DataTypeInt32>();
//    auto const_node = actions_dag->addInput(ColumnWithTypeAndName(int_type->createColumnConst(1, 4), int_type, "_1"));
//    actions_dag->addOrReplaceInIndex(const_node);
    std::string empty_string;
    std::vector<std::string> args = {"x1", "x2"};
    const auto & filter_node = actions_dag->addFunction(std::move(getFunction("less")), getArguments(actions_dag->getIndex(), args), std::move(empty_string));
    actions_dag->getIndex().push_back(&filter_node);
    DataStream input_stream = DataStream{.header=header};
    auto filter = std::make_unique<FilterStep>(input_stream, actions_dag, std::move(filter_node.result_name), true);
    return std::move(filter);
}

int main(int argc, char ** argv)
{
    registerAllFunctions();
    auto & factory = FunctionFactory::instance();
    std::ifstream ifs("/Users/neng.liu/Documents/GitHub/ClickHouse/utils/local-engine/table.json");
    IStreamWrapper isw(ifs);

    Document d;
    d.ParseStream(isw);
    auto cols = getColumns(d);
    auto header = getTableHeader(*cols);
//    std::for_each(header.getNames().begin(), header.getNames().end(), [](const std::string & name) {
//        std::cout << name << std::endl;
//    });

    QueryPlan query_plan;
//    auto source = getSource("/Users/neng.liu/Documents/GitHub/ClickHouse/utils/local-engine/table.csv", header);
    auto x1 = ColumnInt32::create({1,2,3});
    auto x2 = ColumnInt32::create({3,2,1});
    Columns columns;
    columns.emplace_back(std::move(x1));
    columns.emplace_back(std::move(x2));
    Chunk chunk(std::move(columns),3);
    auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
    std::shared_ptr<QueryPipelines> query_pipelines = std::make_shared<QueryPipelines>();

    auto source_step = std::make_unique<ReadFromStorageStep>(Pipe(source), "CSV");
    query_plan.addStep(std::move(source_step));

    auto filter = buildFilter(header);
    query_plan.addStep(std::move(filter));
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan=false};
    auto query_pipline = query_plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings());

    auto buffer = WriteBufferFromFile("/Users/neng.liu/Documents/GitHub/ClickHouse/output.txt");
    auto output = std::make_shared<CSVRowOutputFormat>(buffer, header, true, RowOutputFormatParams(), FormatSettings());
    query_pipline->setOutputFormat(output);
    auto executor = query_pipline->execute();
    executor->execute(1);
//    auto chunk = source->generate();
//    std::cout << chunk.getNumRows();
}

//    auto col = ColumnUInt8::create(1, 1);
//    Columns columns;
//    columns.emplace_back(std::move(col));
//    Chunk chunk(std::move(columns), 1);
//
//    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")};
//
//    auto source = std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk));
//    auto sink = std::make_shared<NullSink>(source->getPort().getHeader());
//
//    connect(source->getPort(), sink->getPort());
//
//    Processors processors;
//    processors.emplace_back(std::move(source));
//    processors.emplace_back(std::move(sink));
//
//    PipelineExecutor executor(processors);
//    executor.execute(1);
