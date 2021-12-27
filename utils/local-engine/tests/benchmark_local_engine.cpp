#include <benchmark/benchmark.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>
#include <Parser/SparkColumnToCHColumn.h>
#include <Parser/CHColumnToSparkRow.h>

using namespace dbms;

bool inside_main=true;

// Define another benchmark
static void BM_CHColumnToSparkRow(benchmark::State& state) {
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder.column("l_orderkey", "I64")
                          .column("l_partkey", "I64")
                          .column("l_suppkey", "I64")
                          .column("l_linenumber", "I32")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                          //                      .column("l_returnflag", "String")
                          //                      .column("l_linestatus", "String")
                          .column("l_shipdate_new", "FP64")
                          .column("l_commitdate_new", "FP64")
                          .column("l_receiptdate_new", "FP64")
                          //                      .column("l_shipinstruct", "String")
                          //                      .column("l_shipmode", "String")
                          //                      .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan = plan_builder.read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();

        dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        dbms::LocalExecutor local_executor;
        state.ResumeTiming();
        local_executor.execute(std::move(query_plan));
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        }
    }
}

static void BM_SimpleAggregate(benchmark::State& state) {
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder.column("l_orderkey", "I64")
                          .column("l_partkey", "I64")
                          .column("l_suppkey", "I64")
                          .column("l_linenumber", "I32")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                          //                      .column("l_returnflag", "String")
                          //                      .column("l_linestatus", "String")
                          .column("l_shipdate_new", "FP64")
                          .column("l_commitdate_new", "FP64")
                          .column("l_receiptdate_new", "FP64")
                          //                      .column("l_shipinstruct", "String")
                          //                      .column("l_shipmode", "String")
                          //                      .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(5)});
        auto plan = plan_builder.registerSupportedFunctions().aggregate({}, {measure}).read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();
        dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        dbms::LocalExecutor local_executor;
        state.ResumeTiming();
        local_executor.execute(std::move(query_plan));
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        }
    }
}

static void BM_CHColumnToSparkRowWithString(benchmark::State& state) {
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder.column("l_orderkey", "I64")
                          .column("l_partkey", "I64")
                          .column("l_suppkey", "I64")
                          .column("l_linenumber", "I32")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                                                .column("l_returnflag", "String")
                                                .column("l_linestatus", "String")
                          .column("l_shipdate_new", "FP64")
                          .column("l_commitdate_new", "FP64")
                          .column("l_receiptdate_new", "FP64")
                                                .column("l_shipinstruct", "String")
                                                .column("l_shipmode", "String")
                                                .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan = plan_builder.read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();
        dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        dbms::LocalExecutor local_executor;
        state.ResumeTiming();
        local_executor.execute(std::move(query_plan));
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        }
    }
}

static void BM_SparkRowToCHColumn(benchmark::State& state) {
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder.column("l_orderkey", "I64")
                          .column("l_partkey", "I64")
                          .column("l_suppkey", "I64")
                          .column("l_linenumber", "I32")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                          //                      .column("l_returnflag", "String")
                          //                      .column("l_linestatus", "String")
                          .column("l_shipdate_new", "FP64")
                          .column("l_commitdate_new", "FP64")
                          .column("l_receiptdate_new", "FP64")
                          //                      .column("l_shipinstruct", "String")
                          //                      .column("l_shipmode", "String")
                          //                      .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan = plan_builder.read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();

        dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        dbms::LocalExecutor local_executor;

        local_executor.execute(std::move(query_plan));
        local_engine::SparkColumnToCHColumn converter;
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
            state.ResumeTiming();
            auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
}


static void BM_SparkRowToCHColumnWithString(benchmark::State& state) {
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder.column("l_orderkey", "I64")
                          .column("l_partkey", "I64")
                          .column("l_suppkey", "I64")
                          .column("l_linenumber", "I32")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                                                .column("l_returnflag", "String")
                                                .column("l_linestatus", "String")
                          .column("l_shipdate_new", "FP64")
                          .column("l_commitdate_new", "FP64")
                          .column("l_receiptdate_new", "FP64")
                                                .column("l_shipinstruct", "String")
                                                .column("l_shipmode", "String")
                                                .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan = plan_builder.read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();
        dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        dbms::LocalExecutor local_executor;

        local_executor.execute(std::move(query_plan));
        local_engine::SparkColumnToCHColumn converter;
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
            state.ResumeTiming();
            auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
}

BENCHMARK(BM_CHColumnToSparkRow)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SimpleAggregate)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_CHColumnToSparkRowWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumn)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumnWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
int main(int argc, char** argv) {
    dbms::SerializedPlanParser::initFunctionEnv();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
