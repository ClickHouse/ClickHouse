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
                          .column("l_linenumber", "I64")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                          //                      .column("l_returnflag", "String")
                          //                      .column("l_linestatus", "String")
                          .column("l_shipdate", "Date")
                          .column("l_commitdate", "Date")
                          .column("l_receiptdate", "Date")
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
                          .column("l_linenumber", "I64")
                          .column("l_quantity", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_discount", "FP64")
                          .column("l_tax", "FP64")
                          //                      .column("l_returnflag", "String")
                          //                      .column("l_linestatus", "String")
                          .column("l_shipdate", "Date")
                          .column("l_commitdate", "Date")
                          .column("l_receiptdate", "Date")
                          //                      .column("l_shipinstruct", "String")
                          //                      .column("l_shipmode", "String")
                          //                      .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        // sum(l_quantity)
        auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(6)});
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

static void BM_TPCH_Q6(benchmark::State& state) {
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder
//                          .column("l_orderkey", "I64")
//                          .column("l_partkey", "I64")
//                          .column("l_suppkey", "I64")
//                          .column("l_linenumber", "I32")
                          .column("l_discount", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_quantity", "FP64")
//                          .column("l_tax", "FP64")
                          //                      .column("l_returnflag", "String")
                          //                      .column("l_linestatus", "String")
                          .column("l_shipdate", "Date")
//                          .column("l_commitdate", "Date")
//                          .column("l_receiptdate", "Date")
                          //                      .column("l_shipinstruct", "String")
                          //                      .column("l_shipmode", "String")
                          //                      .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto *agg_mul = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(1), dbms::selection(0)});
        auto * measure1 = dbms::measureFunction(dbms::SUM, {agg_mul});
        auto * measure2 = dbms::measureFunction(dbms::SUM, {dbms::selection(1)});
        auto * measure3 = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
        auto plan = plan_builder.registerSupportedFunctions()
                        .aggregate({}, {measure1, measure2, measure3})
                        .project({dbms::selection(2), dbms::selection(1), dbms::selection(0)})
                        .filter(dbms::scalarFunction(dbms::AND, {
                                                                    dbms::scalarFunction(AND, {
                                                                                                  dbms::scalarFunction(AND, {
                                                                                                                                dbms::scalarFunction(AND, {
                                                                                                                                                              dbms::scalarFunction(AND, {
                                                                                                                                                                                            dbms::scalarFunction(AND, {
                                                                                                                                                                                                                          dbms::scalarFunction(AND, {
                                                                                                                                                                                                                                                        scalarFunction(IS_NOT_NULL, {selection(3)}),
                                                                                                                                                                                                                                                        scalarFunction(IS_NOT_NULL, {selection(0)})
                                                                                                                                                                                                                                                    }),
                                                                                                                                                                                                                          scalarFunction(IS_NOT_NULL, {selection(2)})
                                                                                                                                                                                                                      }),
                                                                                                                                                                                            dbms::scalarFunction(GREATER_THAN_OR_EQUAL, {selection(3), literalDate(8766)})
                                                                                                                                                                                        }),
                                                                                                                                                              scalarFunction(LESS_THAN, {selection(3), literalDate(9131)})
                                                                                                                                                          }),
                                                                                                                                scalarFunction(GREATER_THAN_OR_EQUAL, {selection(0), literal(0.05)})
                                                                                                                            }),
                                                                                                  scalarFunction(LESS_THAN_OR_EQUAL, {selection(0), literal(0.07)})
                                                                                              }),
                                                                    scalarFunction(LESS_THAN, {selection(2), literal(24.0)})
                                                                }))
                        .read("/data1/test_output/tpch-data-sf10/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet", std::move(schema)).build();
        dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        dbms::LocalExecutor local_executor;
        state.ResumeTiming();
        local_executor.execute(std::move(query_plan));
        //int rows = 0;
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
            //rows += spark_row_info->getNumRows();
        }
        //std::cout << rows <<std::endl;
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

//BENCHMARK(BM_CHColumnToSparkRow)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SimpleAggregate)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_TPCH_Q6)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(100);
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
