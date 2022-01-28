#include <benchmark/benchmark.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>
#include <local/LocalServer.h>
#include <Parser/SparkColumnToCHColumn.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/TableJoin.h>
#include <Storages/SelectQueryInfo.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Interpreters/Context.h>
#include <cstdlib>
#include <Common/PODArray_fwd.h>

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif


using namespace dbms;

bool inside_main=true;
DB::ContextMutablePtr global_context;

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

static void BM_MergeTreeRead(benchmark::State& state) {
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    global_context->setPath("/home/kyligence/Documents/clickhouse_conf/data/");
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    columns_description.add(ColumnDescription("l_orderkey", int64_type));
    columns_description.add(ColumnDescription("l_partkey", int64_type));
    columns_description.add(ColumnDescription("l_suppkey", int64_type));
    columns_description.add(ColumnDescription("l_linenumber", int32_type));
    columns_description.add(ColumnDescription("l_quantity", double_type));
    columns_description.add(ColumnDescription("l_extendedprice", double_type));
    columns_description.add(ColumnDescription("l_discount", double_type));
    columns_description.add(ColumnDescription("l_tax", double_type));
    columns_description.add(ColumnDescription("l_shipdate_new", double_type));
    columns_description.add(ColumnDescription("l_commitdate_new", double_type));
    columns_description.add(ColumnDescription("l_receiptdate_new", double_type));
    metadata->setColumns(columns_description);
    metadata->partition_key.expression_list_ast = std::make_shared<ASTExpressionList>();
    metadata->sorting_key = KeyDescription::getSortingKeyFromAST(makeASTFunction("tuple"), columns_description, global_context, {});
    metadata->primary_key.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>());
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = std::make_unique<DB::MergeTreeSettings>();
    settings->set("min_bytes_for_wide_part", Field(0));
    settings->set("min_rows_for_wide_part", Field(0));

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "test-intel/",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings)
    );
    custom_merge_tree.loadDataParts(false);
    auto sink = std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);
    for (auto _: state)
    {
        state.PauseTiming();
        SelectQueryInfo query_info;
        query_info.query = std::make_shared<ASTSelectQuery>();
        auto syntax_analyzer_result = std::make_shared<TreeRewriterResult>(sink->getPort().getHeader().getNamesAndTypesList());
        syntax_analyzer_result->analyzed_join = std::make_shared<TableJoin>();
        query_info.syntax_analyzer_result = syntax_analyzer_result;
        auto query = custom_merge_tree.reader.read(sink->getPort().getHeader().getNames(),
                                      metadata,
                                      query_info,
                                      global_context,
                                      10000,
                                      1,
                                      QueryProcessingStage::FetchColumns);
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        QueryPipeline query_pipeline;
        query_pipeline.init(query->convertToPipe(optimization_settings, BuildQueryPipelineSettings()));
        state.ResumeTiming();
        auto executor = PullingPipelineExecutor(query_pipeline);
        Chunk chunk;
        while(executor.pull(chunk))
        {
            auto rows = chunk.getNumRows();
            continue;
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
        // sum(l_quantity)
        auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(6)});
        auto plan = plan_builder.registerSupportedFunctions().aggregate({}, {measure}).read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();
        dbms::SerializedPlanParser parser(global_context);
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
                          .column("l_shipdate_new", "Date")
//                          .column("l_commitdate_new", "FP64")
//                          .column("l_receiptdate_new", "FP64")
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
                        .read("/home/kyligence/Documents/test-dataset/intel-gazelle-test-"+std::to_string(state.range(0))+".snappy.parquet", std::move(schema)).build();
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

static void BM_SIMDFilter(benchmark::State& state)
{
    const int n = 10000000;
    for (auto _: state)
    {
        state.PauseTiming();
        PaddedPODArray<Int32> arr;
        PaddedPODArray<UInt8> condition;
        PaddedPODArray<Int32> res_data;
        arr.reserve(n);
        condition.reserve(n);
        res_data.reserve(n);
        for (int i=0; i<n; i++)
        {
            arr.push_back(i);
            condition.push_back(state.range(0));
        }
        const Int32 * data_pos = arr.data();
        const UInt8 * filt_pos =  condition.data();
        state.ResumeTiming();
#ifdef __SSE2__
        int size =n;
        static constexpr size_t SIMD_BYTES = 16;
        const __m128i zero16 = _mm_setzero_si128();
        const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

        while (filt_pos < filt_end_sse)
        {
            UInt16 mask = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));
            mask = ~mask;

            if (0 == mask)
            {
                /// Nothing is inserted.
            }
            else if (0xFFFF == mask)
            {
                res_data.insert(data_pos, data_pos + SIMD_BYTES);
            }
            else
            {
                for (size_t i = 0; i < SIMD_BYTES; ++i)
                    if (filt_pos[i])
                        data_pos[i];
            }

            filt_pos += SIMD_BYTES;
            data_pos += SIMD_BYTES;
        }
#endif
    }
}

static void BM_NormalFilter(benchmark::State& state)
{
    const int n = 10000000;
    for (auto _: state)
    {
        state.PauseTiming();
        PaddedPODArray<Int32> arr;
        PaddedPODArray<UInt8> condition;
        PaddedPODArray<Int32> res_data;
        arr.reserve(n);
        condition.reserve(n);
        res_data.reserve(n);
        for (int i=0; i<n; i++)
        {
            arr.push_back(i);
            condition.push_back(state.range(0));
        }
        const Int32 * data_pos = arr.data();
        const UInt8 * filt_pos =  condition.data();
        const UInt8 * filt_end = filt_pos + n;
        state.ResumeTiming();
        while (filt_pos < filt_end)
        {
            if (*filt_pos)
                res_data.push_back(*data_pos);

            ++filt_pos;
            ++data_pos;
        }
    }
}

//BENCHMARK(BM_CHColumnToSparkRow)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_MergeTreeRead)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_SimpleAggregate)->Arg(3)->Unit(benchmark::kMillisecond)->Iterations(40);
BENCHMARK(BM_SIMDFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
BENCHMARK(BM_NormalFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_TPCH_Q6)->Arg(1)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_CHColumnToSparkRowWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumn)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumnWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
int main(int argc, char** argv) {
    auto shared_context = Context::createShared();
    DB::LocalServer localServer;
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    dbms::SerializedPlanParser::initFunctionEnv();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
