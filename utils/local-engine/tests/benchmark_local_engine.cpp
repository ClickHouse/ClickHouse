#include <benchmark/benchmark.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>
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
#include <Common/MergeTreeTool.h>
#include <Common/Logger.h>
#include <common/logger_useful.h>
#include <Common/Stopwatch.h>

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
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    const auto * type_string = "columns format version: 1\n"
                         "4 columns:\n"
                         "`l_quantity` Float64\n"
                         "`l_extendedprice` Float64\n"
                         "`l_discount` Float64\n"
                         "`l_shipdate_new` Float64\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    metadata = local_engine::buildMetaData(names_and_types_list, global_context);
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = local_engine::buildMergeTreeSettings();

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "tpch-mergetree/lineorder/",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings));
    custom_merge_tree.loadDataParts(false);
    for (auto _: state)
    {
        state.PauseTiming();
        auto query_info = local_engine::buildQueryInfo(names_and_types_list);
        auto data_parts = custom_merge_tree.getDataPartsVector();
        int min_block = 0;
        int max_block = state.range(10);
        MergeTreeData::DataPartsVector selected_parts;
        std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                       [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block <= max_block;});
        auto query = custom_merge_tree.reader.readFromParts(selected_parts,
                                                            names_and_types_list.getNames(),
                                                            metadata,
                                                            metadata,
                                                            *query_info,
                                                            global_context,
                                                            10000,
                                                            1);
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        QueryPipeline query_pipeline;
        query_pipeline.init(query->convertToPipe(optimization_settings, BuildQueryPipelineSettings()));
        state.ResumeTiming();
        auto executor = PullingPipelineExecutor(query_pipeline);
        Chunk chunk;
        while(executor.pull(chunk))
        {
            auto rows = chunk.getNumRows();
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
                          .column("l_discount", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_quantity", "FP64")
                          .column("l_shipdate_new", "Date")
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


static void BM_MERGE_TREE_TPCH_Q6(benchmark::State& state) {
    SerializedPlanParser::global_context = global_context;
    dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder
                          .column("l_discount", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_quantity", "FP64")
                          .column("l_shipdate_new", "FP64")
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
                                                                                                                                                                                            dbms::scalarFunction(GREATER_THAN_OR_EQUAL, {selection(3), literal(8766.0)})
                                                                                                                                                                                        }),
                                                                                                                                                              scalarFunction(LESS_THAN, {selection(3), literal(9131.0)})
                                                                                                                                                          }),
                                                                                                                                scalarFunction(GREATER_THAN_OR_EQUAL, {selection(0), literal(0.05)})
                                                                                                                            }),
                                                                                                  scalarFunction(LESS_THAN_OR_EQUAL, {selection(0), literal(0.07)})
                                                                                              }),
                                                                    scalarFunction(LESS_THAN, {selection(2), literal(24.0)})
                                                                }))
                        .readMergeTree("default", "test", "tpch-mergetree/lineorder/", 1, 12, std::move(schema)).build();

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
static void BM_TestCreateExecute(benchmark::State& state)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto schema = schema_builder
                      .column("l_discount", "FP64")
                      .column("l_extendedprice", "FP64")
                      .column("l_quantity", "FP64")
                      .column("l_shipdate_new", "Date")
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
                    .readMergeTree("default", "test", "home/saber/Documents/data/mergetree", 1, 4, std::move(schema)).build();
    std::string plan_string = plan->SerializeAsString();
    dbms::SerializedPlanParser::global_context = global_context;
    dbms::SerializedPlanParser::global_context->setConfig(dbms::SerializedPlanParser::config);
    for (auto _: state)
    {
        Stopwatch stopwatch;
        stopwatch.start();
        auto context = Context::createCopy(dbms::SerializedPlanParser::global_context);
        context->setPath("/");
        auto _context = stopwatch.elapsedMicroseconds();
        dbms::SerializedPlanParser parser(context);
        auto query_plan = parser.parse(plan_string);
        auto _parser = stopwatch.elapsedMicroseconds() - _context;
        dbms::LocalExecutor * executor = new dbms::LocalExecutor(parser.query_context);
        auto _executor = stopwatch.elapsedMicroseconds() - _parser;
        executor->execute(std::move(query_plan));
        auto _execute = stopwatch.elapsedMicroseconds() - _executor;
        LOG_DEBUG(&Poco::Logger::root(), "create context: {}, create parser: {}, create executor: {}, execute executor: {}", _context, _parser, _executor, _execute);
    }
}
//BENCHMARK(BM_CHColumnToSparkRow)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_MergeTreeRead)->Arg(11)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_SimpleAggregate)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_SIMDFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_NormalFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_TPCH_Q6)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_MERGE_TREE_TPCH_Q6)->Unit(benchmark::kMillisecond)->Iterations(100);
//BENCHMARK(BM_CHColumnToSparkRowWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumn)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumnWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_TestCreateExecute)->Unit(benchmark::kMillisecond)->Iterations(1000);
int main(int argc, char** argv) {
    local_engine::Logger::initConsoleLogger();
    SharedContextHolder shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setConfig(dbms::SerializedPlanParser::config);
    auto path = MERGETREE_DATA(/);
    global_context->setPath(path);
    dbms::SerializedPlanParser::initFunctionEnv();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
