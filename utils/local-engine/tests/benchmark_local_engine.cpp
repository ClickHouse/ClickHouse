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
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Interpreters/Context.h>
#include <cstdlib>
#include <Common/PODArray_fwd.h>
#include <Common/MergeTreeTool.h>
#include <Common/Logger.h>
#include <base/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Functions/FunctionFactory.h>
#include <Shuffle/ShuffleSplitter.h>
#include <Shuffle/ShuffleReader.h>
#include <Interpreters/HashJoin.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/BatchParquetFileSource.h>




#if defined(__SSE2__)
#    include <emmintrin.h>
#endif


using namespace local_engine;
using namespace dbms;

bool inside_main=true;
DB::ContextMutablePtr global_context;
// Define another benchmark+

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
                          .column("l_returnflag", "String")
                          .column("l_linestatus", "String")
                          .column("l_shipdate", "Date")
                          .column("l_commitdate", "Date")
                          .column("l_receiptdate", "Date")
                          .column("l_shipinstruct", "String")
                          .column("l_shipmode", "String")
                          .column("l_comment", "String")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan = plan_builder.readMergeTree("default", "test", "home/saber/Documents/data/mergetree", 1, 10, std::move(schema)).build();
        local_engine::SerializedPlanParser parser(global_context);
        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;
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
                               "16 columns:\n"
                               "`l_orderkey` Int64\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                                                              "`l_returnflag` String\n"
                                                              "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                                   "`l_shipinstruct` String\n"
                                   "`l_shipmode` String\n"
                                   "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    metadata = local_engine::buildMetaData(names_and_types_list, global_context);
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = local_engine::buildMergeTreeSettings();

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "home/admin1/Documents/data/tpch/mergetree/lineitem",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings));
    auto snapshot = std::make_shared<StorageSnapshot>(custom_merge_tree, metadata);
    custom_merge_tree.loadDataParts(false);
    for (auto _: state)
    {
        state.PauseTiming();
        auto query_info = local_engine::buildQueryInfo(names_and_types_list);
        auto data_parts = custom_merge_tree.getDataPartsVector();
        int min_block = 0;
        int max_block = state.range(0);
        MergeTreeData::DataPartsVector selected_parts;
        std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                       [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block <= max_block;});
        auto query = custom_merge_tree.reader.readFromParts(selected_parts,
                                                            names_and_types_list.getNames(),
                                                            snapshot,
                                                            *query_info,
                                                            global_context,
                                                            10000,
                                                            1);
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        QueryPipelineBuilder query_pipeline;
        query_pipeline.init(query->convertToPipe(optimization_settings, BuildQueryPipelineSettings()));
        state.ResumeTiming();
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(query_pipeline));
        auto executor = PullingPipelineExecutor(pipeline);
        Chunk chunk;
        int sum =0;
        while(executor.pull(chunk))
        {
            sum+= chunk.getNumRows();
        }
    }
}

static void BM_ParquetRead(benchmark::State& state) {


    const auto * type_string = "columns format version: 1\n"
                               "16 columns:\n"
                               "`l_orderkey` Int64\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                               "`l_shipinstruct` String\n"
                               "`l_shipmode` String\n"
                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    ColumnsWithTypeAndName columns;
    for (const auto & item : names_and_types_list)
    {
        ColumnWithTypeAndName col;
        col.column = item.type->createColumn();
        col.type = item.type;
        col.name = item.name;
        columns.emplace_back(std::move(col));
    }
    auto header = Block(std::move(columns));

    for (auto _: state)
    {
        auto files = std::make_shared<FilesInfo>();
        files->files = {
            "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet",
            "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00001-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet",
            "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00002-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet",
        };
        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(Pipe(std::make_shared<BatchParquetFileSource>(files, header, SerializedPlanParser::global_context)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto executor = PullingPipelineExecutor(pipeline);
        auto result = header.cloneEmpty();
        size_t total_rows = 0;
        while (executor.pull(result))
        {
            total_rows += result.rows();
        }
        std::cerr << "rows:" << total_rows << std::endl;
    }
}

static void BM_ShuffleSplitter(benchmark::State& state) {
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    const auto * type_string = "columns format version: 1\n"
                               "15 columns:\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                               "`l_shipinstruct` String\n"
                               "`l_shipmode` String\n"
                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    metadata = local_engine::buildMetaData(names_and_types_list, global_context);
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = local_engine::buildMergeTreeSettings();

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "home/saber/Documents/data/mergetree",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings));
    custom_merge_tree.loadDataParts(false);
    auto snapshot = std::make_shared<StorageSnapshot>(custom_merge_tree, metadata);
    for (auto _: state)
    {
        state.PauseTiming();
        auto query_info = local_engine::buildQueryInfo(names_and_types_list);
        auto data_parts = custom_merge_tree.getDataPartsVector();
        int min_block = 0;
        int max_block = state.range(0);
        MergeTreeData::DataPartsVector selected_parts;
        std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                     [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block <= max_block;});
        auto query = custom_merge_tree.reader.readFromParts(selected_parts,
                                                            names_and_types_list.getNames(),
                                                            snapshot,
                                                            *query_info,
                                                            global_context,
                                                            10000,
                                                            1);
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        QueryPipelineBuilder query_pipeline;
        query_pipeline.init(query->convertToPipe(optimization_settings, BuildQueryPipelineSettings()));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(query_pipeline));
        state.ResumeTiming();
        auto executor = PullingPipelineExecutor(pipeline);
        Block chunk = executor.getHeader();
        int sum =0;
        auto root = "/tmp/test_shuffle/"+local_engine::ShuffleSplitter::compress_methods[state.range(1)];
        local_engine::SplitOptions options{
            .buffer_size = 8192,
            .data_file = root+"/data.dat",
            .local_tmp_dir = root,
            .map_id = 1,
            .partition_nums = 4,
            .compress_method = local_engine::ShuffleSplitter::compress_methods[state.range(1)]
        };
        auto splitter = local_engine::ShuffleSplitter::create("rr", options);
        while(executor.pull(chunk))
        {
            sum += chunk.rows();
            splitter->split(chunk);
        }
        splitter->stop();
        splitter->writeIndexFile();
        std::cout << sum <<"\n";
    }
}

static void BM_HashShuffleSplitter(benchmark::State& state) {
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    const auto * type_string = "columns format version: 1\n"
                               "15 columns:\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                               "`l_shipinstruct` String\n"
                               "`l_shipmode` String\n"
                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    metadata = local_engine::buildMetaData(names_and_types_list, global_context);
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = local_engine::buildMergeTreeSettings();

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "home/saber/Documents/data/mergetree",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings));
    custom_merge_tree.loadDataParts(false);
    auto snapshot = std::make_shared<StorageSnapshot>(custom_merge_tree, metadata);

    for (auto _: state)
    {
        state.PauseTiming();
        auto query_info = local_engine::buildQueryInfo(names_and_types_list);
        auto data_parts = custom_merge_tree.getDataPartsVector();
        int min_block = 0;
        int max_block = state.range(0);
        MergeTreeData::DataPartsVector selected_parts;
        std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                     [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block <= max_block;});
        auto query = custom_merge_tree.reader.readFromParts(selected_parts,
                                                            names_and_types_list.getNames(),
                                                            snapshot,
                                                            *query_info,
                                                            global_context,
                                                            10000,
                                                            1);
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        QueryPipelineBuilder query_pipeline;
        query_pipeline.init(query->convertToPipe(optimization_settings, BuildQueryPipelineSettings()));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(query_pipeline));
        state.ResumeTiming();
        auto executor = PullingPipelineExecutor(pipeline);
        Block chunk = executor.getHeader();
        int sum =0;
        auto root = "/tmp/test_shuffle/"+local_engine::ShuffleSplitter::compress_methods[state.range(1)];
        local_engine::SplitOptions options{
            .buffer_size = 8192,
            .data_file = root+"/data.dat",
            .local_tmp_dir = root,
            .map_id = 1,
            .partition_nums = 4,
            .exprs = {"l_partkey","l_suppkey"},
            .compress_method = local_engine::ShuffleSplitter::compress_methods[state.range(1)]
        };
        auto splitter = local_engine::ShuffleSplitter::create("hash", options);
        while(executor.pull(chunk))
        {
            sum += chunk.rows();
            splitter->split(chunk);
        }
        splitter->stop();
        splitter->writeIndexFile();
        std::cout << sum <<"\n";
    }
}
static void BM_ShuffleReader(benchmark::State& state)
{
    for (auto _: state)
    {
        auto read_buffer = std::make_unique<ReadBufferFromFile>("/tmp/test_shuffle/ZSTD/data.dat");
//        read_buffer->seek(357841655, SEEK_SET);
        auto shuffle_reader = local_engine::ShuffleReader(std::move(read_buffer), true);
        Block* block;
        int sum = 0;
        do
        {
            block = shuffle_reader.read();
            sum += block->rows();
        }
        while(block->columns() != 0);
        std::cout << "total rows:" << sum << std::endl;
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
        local_engine::SerializedPlanParser parser(global_context);
        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;
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
        local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;
        state.ResumeTiming();
        local_executor.execute(std::move(query_plan));
        while (local_executor.hasNext())
        {
            Block * block = local_executor.nextColumnar();
            delete block;
        }
    }
}


static void BM_MERGE_TREE_TPCH_Q6(benchmark::State& state) {
    SerializedPlanParser::global_context = global_context;
    local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder
                          .column("l_discount", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_quantity", "FP64")
                          .column("l_shipdate", "Date")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto *agg_mul = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(1), dbms::selection(0)});
        auto * measure1 = dbms::measureFunction(dbms::SUM, {agg_mul});
//        auto * measure2 = dbms::measureFunction(dbms::SUM, {dbms::selection(1)});
//        auto * measure3 = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
        auto plan = plan_builder.registerSupportedFunctions()
                        .aggregate({}, {measure1})
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
                        .readMergeTree("default", "test", "home/saber/Documents/data/mergetree/", 1, 4, std::move(schema)).build();

        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;
        state.ResumeTiming();
        local_executor.execute(std::move(query_plan));
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        }
    }
}

static void BM_MERGE_TREE_TPCH_Q6_NEW(benchmark::State& state) {
    SerializedPlanParser::global_context = global_context;
    local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
    for (auto _: state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder
                          .column("l_discount", "FP64")
                          .column("l_extendedprice", "FP64")
                          .column("l_quantity", "FP64")
                          .column("l_shipdate", "Date")
                          .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto *agg_mul = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(1), dbms::selection(0)});

        auto * measure1 = dbms::measureFunction(dbms::SUM, {dbms::selection(0)});
//        auto * measure2 = dbms::measureFunction(dbms::SUM, {dbms::selection(1)});
//        auto * measure3 = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
        auto plan = plan_builder.registerSupportedFunctions()
                        .aggregate({}, {measure1})
                        .project({agg_mul})
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
                        .readMergeTree("default", "test", "home/saber/Documents/data/mergetree/", 1, 4, std::move(schema)).build();

        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;
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
        local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;
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

        local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;

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
        local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
        auto query_plan = parser.parse(std::move(plan));
        local_engine::LocalExecutor local_executor;

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
    local_engine::SerializedPlanParser::global_context = global_context;
    local_engine::SerializedPlanParser::global_context->setConfig(local_engine::SerializedPlanParser::config);
    for (auto _: state)
    {
        Stopwatch stopwatch;
        stopwatch.start();
        auto context = Context::createCopy(local_engine::SerializedPlanParser::global_context);
        context->setPath("/");
        auto _context = stopwatch.elapsedMicroseconds();
        local_engine::SerializedPlanParser parser(context);
        auto query_plan = parser.parse(plan_string);
        auto _parser = stopwatch.elapsedMicroseconds() - _context;
        local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context);
        auto _executor = stopwatch.elapsedMicroseconds() - _parser;
        executor->execute(std::move(query_plan));
        auto _execute = stopwatch.elapsedMicroseconds() - _executor;
        LOG_DEBUG(&Poco::Logger::root(), "create context: {}, create parser: {}, create executor: {}, execute executor: {}", _context, _parser, _executor, _execute);
    }
}


int add(int a, int b)
{
    return a + b;
}



static void BM_TestSum(benchmark::State& state)
{
    int cnt = state.range(0);
    int i = 0;
    std::vector<int> x;
    std::vector<int> y;
    x.reserve(cnt);
    x.assign(cnt, 2);
    y.reserve(cnt);

    for (auto _: state)
    {
        for(i=0;i<cnt;i++){
            y[i] = add(x[i], i);
        }
    }
}


static void BM_TestSumInline(benchmark::State& state)
{
    int cnt = state.range(0);
    int i = 0;
    std::vector<int> x;
    std::vector<int> y;
    x.reserve(cnt);
    x.assign(cnt, 2);
    y.reserve(cnt);

    for (auto _: state)
    {
        for(i=0;i<cnt;i++){
            y[i] =x[i]+i;
        }
    }
}

static void BM_TestPlus(benchmark::State& state)
{
    UInt64 rows = state.range(0);
    auto & factory = FunctionFactory::instance();
    auto & type_factory = DataTypeFactory::instance();
    auto plus = factory.get("plus", global_context);
    auto type = type_factory.get("UInt64");
    ColumnsWithTypeAndName arguments;
    arguments.push_back(ColumnWithTypeAndName(type, "x"));
    arguments.push_back(ColumnWithTypeAndName(type, "y"));
    auto function = plus->build(arguments);

    ColumnsWithTypeAndName arguments_with_data;
    Block block;
    auto x = ColumnWithTypeAndName(type, "x");
    auto y = ColumnWithTypeAndName(type, "y");
    MutableColumnPtr mutable_x = x.type->createColumn();
    MutableColumnPtr mutable_y = y.type->createColumn();
    mutable_x->reserve(rows);
    mutable_y->reserve(rows);
    ColumnVector<UInt64> & column_x = assert_cast<ColumnVector<UInt64> &>(*mutable_x);
    ColumnVector<UInt64> & column_y = assert_cast<ColumnVector<UInt64> &>(*mutable_y);
    for (UInt64 i=0; i<rows; i++)
    {
        column_x.insertValue(i);
        column_y.insertValue(i+1);
    }
    x.column = std::move(mutable_x);
    y.column = std::move(mutable_y);
    block.insert(x);
    block.insert(y);
    auto executable_function = function->prepare(arguments);
    for (auto _: state)
    {
        auto result = executable_function->execute(block.getColumnsWithTypeAndName(),
                                                   type, rows, false);
    }
}

static void BM_TestPlusEmbedded(benchmark::State& state)
{
    UInt64 rows = state.range(0);
    auto & factory = FunctionFactory::instance();
    auto & type_factory = DataTypeFactory::instance();
    auto plus = factory.get("plus", global_context);
    auto type = type_factory.get("UInt64");
    ColumnsWithTypeAndName arguments;
    arguments.push_back(ColumnWithTypeAndName(type, "x"));
    arguments.push_back(ColumnWithTypeAndName(type, "y"));
    auto function = plus->build(arguments);
    ColumnsWithTypeAndName arguments_with_data;
    Block block;
    auto x = ColumnWithTypeAndName(type, "x");
    auto y = ColumnWithTypeAndName(type, "y");
    MutableColumnPtr mutable_x = x.type->createColumn();
    MutableColumnPtr mutable_y = y.type->createColumn();
    mutable_x->reserve(rows);
    mutable_y->reserve(rows);
    ColumnVector<UInt64> & column_x = assert_cast<ColumnVector<UInt64> &>(*mutable_x);
    ColumnVector<UInt64> & column_y = assert_cast<ColumnVector<UInt64> &>(*mutable_y);
    for (UInt64 i=0; i<rows; i++)
    {
        column_x.insertValue(i);
        column_y.insertValue(i+1);
    }
    x.column = std::move(mutable_x);
    y.column = std::move(mutable_y);
    block.insert(x);
    block.insert(y);
    CHJIT chjit;
    auto compiled_function = compileFunction(chjit, *function);
    std::vector<ColumnData> columns(arguments.size() + 1);
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        auto column = block.getByPosition(i).column->convertToFullColumnIfConst();
        columns[i] = getColumnData(column.get());
    }
    for (auto _: state)
    {
        auto result_column = type->createColumn();
        result_column->reserve(rows);
        columns[arguments.size()] = getColumnData(result_column.get());
        compiled_function.compiled_function(rows, columns.data());
    }
}

static void BM_TestReadColumn(benchmark::State& state)
{
    for (auto _ : state)
    {
        ReadBufferFromFile data_buf("/home/saber/Documents/test/c151.bin", 100000);
        CompressedReadBuffer compressed(data_buf);
        ReadBufferFromFile buf("/home/saber/Documents/test/c151.mrk2");
        while(!buf.eof() && !data_buf.eof()) {
            size_t x;
            size_t y;
            size_t z;
            readIntBinary(x, buf);
            readIntBinary(y, buf);
            readIntBinary(z, buf);
            std::cout << std::to_string(x) +" " << std::to_string(y) + " " << std::to_string(z) + " "  <<  "\n";
            data_buf.seek(x, SEEK_SET);
            assert(!data_buf.eof());
            std::string data;
            data.reserve(y);
            compressed.readBig(reinterpret_cast<char*>(data.data()), y);
            std::cout << data << "\n";
        }
    }
}

double quantile(const vector<double>&x)
{
    double q = 0.8;
    assert(q >= 0.0 && q <= 1.0);
    const int n = x.size();
    double id = (n-1)*q;
    int lo = floor(id);
    int hi = ceil(id);
    double qs = x[lo];
    double h = (id-lo);
    return (1.0 - h) * qs + h * x[hi];
}

// compress benchmark
#include <string.h>
#include <optional>
#include <base/types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/CompressionInfo.h>
#include <IO/WriteHelpers.h>
#include <Compression/LZ4_decompress_faster.h>
#include <IO/copyData.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/memcpySmall.h>
#include <base/unaligned.h>
namespace DB 
{
class FasterCompressedReadBufferBase
{
protected:
    ReadBuffer * compressed_in;

    /// If 'compressed_in' buffer has whole compressed block - then use it. Otherwise copy parts of data to 'own_compressed_buffer'.
    PODArray<char> own_compressed_buffer;
    /// Points to memory, holding compressed block.
    char * compressed_buffer = nullptr;

    ssize_t variant;

    /// Variant for reference implementation of LZ4.
    static constexpr ssize_t LZ4_REFERENCE = -3;

    LZ4::StreamStatistics stream_stat;
    LZ4::PerformanceStatistics perf_stat;

    size_t readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
    {
        if (compressed_in->eof())
            return 0;

        CityHash_v1_0_2::uint128 checksum;
        compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

        own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
        compressed_in->readStrict(&own_compressed_buffer[0], COMPRESSED_BLOCK_HEADER_SIZE);

        UInt8 method = own_compressed_buffer[0];    /// See CompressedWriteBuffer.h

        size_t & size_compressed = size_compressed_without_checksum;

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4) ||
            method == static_cast<UInt8>(CompressionMethodByte::ZSTD) ||
            method == static_cast<UInt8>(CompressionMethodByte::NONE))
        {
            size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
            size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);
        }
        else
            throw runtime_error("Unknown compression method: " + toString(method));

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw runtime_error("Too large size_compressed. Most likely corrupted data.");

        /// Is whole compressed block located in 'compressed_in' buffer?
        if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE &&
            compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
        {
            compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
            compressed_buffer = compressed_in->position();
            compressed_in->position() += size_compressed;
        }
        else
        {
            own_compressed_buffer.resize(size_compressed + (variant == LZ4_REFERENCE ? 0 : LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER));
            compressed_buffer = &own_compressed_buffer[0];
            compressed_in->readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
        }

        return size_compressed + sizeof(checksum);
    }

    void decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
    {
        UInt8 method = compressed_buffer[0];    /// See CompressedWriteBuffer.h

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
        {
            //LZ4::statistics(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed, stat);
            LZ4::decompress(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_compressed_without_checksum, size_decompressed, perf_stat);
        }
        else
            throw runtime_error("Unknown compression method: " + toString(method));
    }

public:
    /// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
    FasterCompressedReadBufferBase(ReadBuffer * in, ssize_t variant_)
        : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE), variant(variant_), perf_stat(variant)
    {
    }
    LZ4::StreamStatistics getStreamStatistics() const { return stream_stat; }
    LZ4::PerformanceStatistics getPerformanceStatistics() const { return perf_stat; }
};


class FasterCompressedReadBuffer : public FasterCompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override
    {
        size_t size_decompressed;
        size_t size_compressed_without_checksum;
        size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
        if (!size_compressed)
            return false;

        memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
        working_buffer = Buffer(&memory[0], &memory[size_decompressed]);

        decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

        return true;
    }

public:
    FasterCompressedReadBuffer(ReadBuffer & in_, ssize_t method)
        : FasterCompressedReadBufferBase(&in_, method), BufferWithOwnMemory<ReadBuffer>(0)
    {
    }
};

}



static void BM_TestDecompress(benchmark::State& state) 
{
    std::vector<String> files = {
        "/home/saber/Documents/data/mergetree/all_1_1_0/l_discount.bin",
        "/home/saber/Documents/data/mergetree/all_1_1_0/l_extendedprice.bin",
        "/home/saber/Documents/data/mergetree/all_1_1_0/l_quantity.bin",
        "/home/saber/Documents/data/mergetree/all_1_1_0/l_shipdate.bin",

        "/home/saber/Documents/data/mergetree/all_2_2_0/l_discount.bin",
        "/home/saber/Documents/data/mergetree/all_2_2_0/l_extendedprice.bin",
        "/home/saber/Documents/data/mergetree/all_2_2_0/l_quantity.bin",
        "/home/saber/Documents/data/mergetree/all_2_2_0/l_shipdate.bin",

        "/home/saber/Documents/data/mergetree/all_3_3_0/l_discount.bin",
        "/home/saber/Documents/data/mergetree/all_3_3_0/l_extendedprice.bin",
        "/home/saber/Documents/data/mergetree/all_3_3_0/l_quantity.bin",
        "/home/saber/Documents/data/mergetree/all_3_3_0/l_shipdate.bin"
    };
    for (auto _ : state)
    {
        for (auto file : files)
        {
            ReadBufferFromFile in(file);
            FasterCompressedReadBuffer decompressing_in(in, state.range(0));
            while (!decompressing_in.eof())
            {
                decompressing_in.position() = decompressing_in.buffer().end();
                decompressing_in.next();
            }
            // std::cout << "call count:" << std::to_string(decompressing_in.getPerformanceStatistics().data[state.range(0)].count) << "\n";
            // std::cout << "false count:" << std::to_string(decompressing_in.false_count) << "\n";
            // decompressing_in.getStreamStatistics().print();
        }

    }
}

#include <Parser/CHColumnToSparkRow.h>


static void BM_CHColumnToSparkRowNew(benchmark::State& state) {
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    const auto * type_string = "columns format version: 1\n"
                               "15 columns:\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                               "`l_shipinstruct` String\n"
                               "`l_shipmode` String\n"
                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    metadata = local_engine::buildMetaData(names_and_types_list, global_context);
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = local_engine::buildMergeTreeSettings();

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "home/saber/Documents/data/tpch/mergetree/lineitem",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings));
    auto snapshot = std::make_shared<StorageSnapshot>(custom_merge_tree, metadata);
    custom_merge_tree.loadDataParts(false);
    for (auto _: state)
    {
        state.PauseTiming();
        auto query_info = local_engine::buildQueryInfo(names_and_types_list);
        auto data_parts = custom_merge_tree.getDataPartsVector();
        int min_block = 0;
        int max_block = 10;
        MergeTreeData::DataPartsVector selected_parts;
        std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                     [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block <= max_block;});
        auto query = custom_merge_tree.reader.readFromParts(selected_parts,
                                                            names_and_types_list.getNames(),
                                                            snapshot,
                                                            *query_info,
                                                            global_context,
                                                            10000,
                                                            1);
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        QueryPipelineBuilder query_pipeline;
        query_pipeline.init(query->convertToPipe(optimization_settings, BuildQueryPipelineSettings()));
        state.ResumeTiming();
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(query_pipeline));
        auto executor = PullingPipelineExecutor(pipeline);
        Block header = executor.getHeader();
        CHColumnToSparkRow converter;
        int sum =0;
        while(executor.pull(header))
        {
            sum+= header.rows();
            auto spark_row = converter.convertCHColumnToSparkRow(header);
            converter.freeMem(spark_row->getBufferAddress(), spark_row->getTotalBytes());
        }
        std::cerr <<"rows: " << sum << std::endl;
    }
}

struct MergeTreeWithSnapshot {
    std::shared_ptr<local_engine::CustomStorageMergeTree> merge_tree;
    std::shared_ptr<StorageSnapshot> snapshot;
    NamesAndTypesList columns;
};

MergeTreeWithSnapshot buildMergeTree(NamesAndTypesList names_and_types, std::string relative_path, std::string table)
{
    auto metadata = local_engine::buildMetaData(names_and_types, global_context);
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = local_engine::buildMergeTreeSettings();
    std::shared_ptr<local_engine::CustomStorageMergeTree> custom_merge_tree = std::make_shared<local_engine::CustomStorageMergeTree>(
        DB::StorageID("default", table), relative_path, *metadata, false, global_context, "", param, std::move(settings));
    auto snapshot = std::make_shared<StorageSnapshot>(*custom_merge_tree, metadata);
    custom_merge_tree->loadDataParts(false);
    return MergeTreeWithSnapshot{.merge_tree = custom_merge_tree, .snapshot = snapshot, .columns = names_and_types};
}

QueryPlanPtr readFromMergeTree(MergeTreeWithSnapshot storage)
{
    auto query_info = local_engine::buildQueryInfo(storage.columns);
    auto data_parts = storage.merge_tree->getDataPartsVector();
//    int min_block = 0;
//    int max_block = 10;
//    MergeTreeData::DataPartsVector selected_parts;
//    std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
//                 [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block <= max_block;});
    return storage.merge_tree->reader.readFromParts(data_parts,
                                                        storage.columns.getNames(),
                                                        storage.snapshot,
                                                        *query_info,
                                                        global_context,
                                                        10000,
                                                        1);
}

QueryPlanPtr joinPlan(QueryPlanPtr left, QueryPlanPtr right, String left_key, String right_key, size_t block_size = 8192)
{
    auto join = std::make_shared<TableJoin>(global_context->getSettings(), global_context->getTemporaryVolume());
    auto left_columns = left->getCurrentDataStream().header.getColumnsWithTypeAndName();
    auto right_columns = right->getCurrentDataStream().header.getColumnsWithTypeAndName();
    join->setKind(ASTTableJoin::Kind::Left);
    join->setStrictness(ASTTableJoin::Strictness::All);
    join->setColumnsFromJoinedTable(right->getCurrentDataStream().header.getNamesAndTypesList());
    join->addDisjunct();
    ASTPtr lkey = std::make_shared<ASTIdentifier>(left_key);
    ASTPtr rkey = std::make_shared<ASTIdentifier>(right_key);
    join->addOnKeys(lkey, rkey);
    for (const auto & column : join->columnsFromJoinedTable())
    {
        join->addJoinedColumn(column);
    }

    auto left_keys = left->getCurrentDataStream().header.getNamesAndTypesList();
    join->addJoinedColumnsAndCorrectTypes(left_keys, true);
    ActionsDAGPtr left_convert_actions = nullptr;
    ActionsDAGPtr right_convert_actions = nullptr;
    std::tie(left_convert_actions, right_convert_actions) = join->createConvertingActions(left_columns, right_columns);

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        right->addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        left->addStep(std::move(converting_step));
    }
    auto hash_join = std::make_shared<HashJoin>(join, right->getCurrentDataStream().header);

    QueryPlanStepPtr join_step = std::make_unique<JoinStep>(
        left->getCurrentDataStream(),
        right->getCurrentDataStream(),
        hash_join,
        block_size);

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::move(left));
    plans.emplace_back(std::move(right));

    auto query_plan = std::make_unique<QueryPlan>();
    query_plan->unitePlans(std::move(join_step), std::move(plans));
    return query_plan;
}

static void BM_JoinTest(benchmark::State& state) {
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    const auto * supplier_type_string = "columns format version: 1\n"
                                        "2 columns:\n"
                                        "`s_suppkey` Int64\n"
                                        "`s_nationkey` Int64\n";
    auto supplier_types = NamesAndTypesList::parse(supplier_type_string);
    auto supplier = buildMergeTree(supplier_types, "home/saber/Documents/data/tpch/mergetree/supplier", "supplier");

    const auto * nation_type_string = "columns format version: 1\n"
                                      "1 columns:\n"
                                      "`n_nationkey` Int64\n";
    auto nation_types = NamesAndTypesList::parse(nation_type_string);
    auto nation = buildMergeTree(nation_types, "home/saber/Documents/data/tpch/mergetree/nation", "nation");


    const auto * partsupp_type_string = "columns format version: 1\n"
                                        "3 columns:\n"
                                        "`ps_suppkey` Int64\n"
                                        "`ps_availqty` Int64\n"
                                        "`ps_supplycost` Float64\n";
    auto partsupp_types = NamesAndTypesList::parse(partsupp_type_string);
    auto partsupp = buildMergeTree(partsupp_types, "home/saber/Documents/data/tpch/mergetree/partsupp", "partsupp");

    for (auto _: state)
    {
        state.PauseTiming();
        QueryPlanPtr supplier_query;
        {
            auto left = readFromMergeTree(partsupp);
            auto right = readFromMergeTree(supplier);
            supplier_query = joinPlan(std::move(left), std::move(right), "ps_suppkey", "s_suppkey");
        }
        auto right = readFromMergeTree(nation);
        auto query_plan = joinPlan(std::move(supplier_query), std::move(right), "s_nationkey", "n_nationkey");
        QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
        BuildQueryPipelineSettings pipeline_settings;
        auto pipeline_builder = query_plan->buildQueryPipeline(optimization_settings, pipeline_settings);
        state.ResumeTiming();
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
        auto executor = PullingPipelineExecutor(pipeline);
        Block header = executor.getHeader();
        CHColumnToSparkRow converter;
        int sum =0;
        while(executor.pull(header))
        {
            sum+= header.rows();
//            auto spark_row = converter.convertCHColumnToSparkRow(header);
//            converter.freeMem(spark_row->getBufferAddress(), spark_row->getTotalBytes());
        }
    }
}

// BENCHMARK(BM_TestDecompress)->Arg(0)->Arg(1)->Arg(2)->Arg(3)->Unit(benchmark::kMillisecond)->Iterations(50)->Repetitions(6)->ComputeStatistics("80%", quantile);

//BENCHMARK(BM_JoinTest)->Unit(benchmark::kMillisecond)->Iterations(10)->Repetitions(250)->ComputeStatistics("80%", quantile);

//BENCHMARK(BM_CHColumnToSparkRow)->Unit(benchmark::kMillisecond)->Iterations(40);
 BENCHMARK(BM_MergeTreeRead)->Arg(10)->Unit(benchmark::kMillisecond)->Iterations(10)->Repetitions(5);
 BENCHMARK(BM_ParquetRead)->Unit(benchmark::kMillisecond)->Iterations(10)->Repetitions(5);

//BENCHMARK(BM_ShuffleSplitter)->Args({2, 0})->Args({2, 1})->Args({2, 2})->Unit(benchmark::kMillisecond)->Iterations(1);
//BENCHMARK(BM_HashShuffleSplitter)->Args({2, 0})->Args({2, 1})->Args({2, 2})->Unit(benchmark::kMillisecond)->Iterations(1);
//BENCHMARK(BM_ShuffleReader)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SimpleAggregate)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_SIMDFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_NormalFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_TPCH_Q6)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_MERGE_TREE_TPCH_Q6)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_MERGE_TREE_TPCH_Q6_NEW)->Unit(benchmark::kMillisecond)->Iterations(10);

//BENCHMARK(BM_CHColumnToSparkRowWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumn)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumnWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_TestCreateExecute)->Unit(benchmark::kMillisecond)->Iterations(1000);
//BENCHMARK(BM_TestReadColumn)->Unit(benchmark::kMillisecond)->Iterations(1);

//BENCHMARK(BM_TestSum)->Arg(1000000)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(100)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();
//BENCHMARK(BM_TestSumInline)->Arg(1000000)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(100)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();
//
//BENCHMARK(BM_TestPlus)->Arg(65505)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(1000)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();
//BENCHMARK(BM_TestPlusEmbedded)->Arg(65505)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(1000)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();



int main(int argc, char** argv) {
//    local_engine::Logger::initConsoleLogger();
    SharedContextHolder shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setConfig(local_engine::SerializedPlanParser::config);
    auto path = "/";
    global_context->setPath(path);
    SerializedPlanParser::global_context = global_context;
    local_engine::SerializedPlanParser::initFunctionEnv();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
