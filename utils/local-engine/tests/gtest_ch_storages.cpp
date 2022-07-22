#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/BatchParquetFileSource.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Common/MergeTreeTool.h>



using namespace DB;
using namespace local_engine;


TEST(TestBatchParquetFileSource, blob)
{
    auto config = local_engine::SerializedPlanParser::config;
    config->setString("blob.storage_account_url", "http://127.0.0.1:10000/devstoreaccount1");
    config->setString("blob.container_name", "libch");
    config->setString("blob.container_already_exists", "true");
    config->setString(
        "blob.connection_string",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey="
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/"
        "devstoreaccount1;");

    auto builder = std::make_unique<QueryPipelineBuilder>();
    auto files = std::make_shared<FilesInfo>();
    files->files = {"wasb://libch/parquet/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet"};
    //    files->files = {"file:///home/saber/Downloads/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet"};

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
    builder->init(Pipe(std::make_shared<BatchParquetFileSource>(files, header, SerializedPlanParser::global_context)));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto executor = PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    bool is_first = true;
    while (executor.pull(result))
    {
        if (is_first)
            debug::headBlock(result);
        total_rows += result.rows();
        is_first = false;
    }

    ASSERT_TRUE(total_rows > 0);
    std::cerr << "rows:" << total_rows << std::endl;
}


TEST(TestBatchParquetFileSource, s3)
{
    auto config = local_engine::SerializedPlanParser::config;
    config->setString("s3.endpoint", "http://localhost:9000/tpch/");
    config->setString("s3.region", "us-east-1");
    config->setString("s3.access_key_id", "admin");
    config->setString("s3.secret_access_key", "password");

    auto builder = std::make_unique<QueryPipelineBuilder>();
    auto files = std::make_shared<FilesInfo>();
    files->files = {"s3://tpch/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet"};
    //    files->files = {"file:///home/saber/Downloads/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet"};

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
    builder->init(Pipe(std::make_shared<BatchParquetFileSource>(files, header, SerializedPlanParser::global_context)));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto executor = PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    bool is_first = true;
    while (executor.pull(result))
    {
        if (is_first)
            debug::headBlock(result);
        total_rows += result.rows();
        is_first = false;
    }

    ASSERT_TRUE(total_rows > 0);
    std::cerr << "rows:" << total_rows << std::endl;
}

TEST(TestBatchParquetFileSource, local_file)
{
    auto builder = std::make_unique<QueryPipelineBuilder>();
    auto files = std::make_shared<FilesInfo>();
    files->files = {
        "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet",
        "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00001-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet",
        "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00002-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet",
    };

    const auto * type_string = "columns format version: 1\n"
                               "2 columns:\n"
//                               "`l_partkey` Int64\n"
//                               "`l_suppkey` Int64\n"
//                               "`l_linenumber` Int32\n"
//                               "`l_quantity` Float64\n"
//                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n";
//                               "`l_returnflag` String\n"
//                               "`l_linestatus` String\n"
//                               "`l_shipdate` Date\n"
//                               "`l_commitdate` Date\n"
//                               "`l_receiptdate` Date\n"
//                               "`l_shipinstruct` String\n"
//                               "`l_shipmode` String\n"
//                               "`l_comment` String\n";
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
    builder->init(Pipe(std::make_shared<BatchParquetFileSource>(files, header, SerializedPlanParser::global_context)));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto executor = PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    bool is_first = true;
    while (executor.pull(result))
    {
        if (is_first)
            debug::headBlock(result);
        total_rows += result.rows();
        is_first = false;
    }
    std::cerr << "rows:" << total_rows << std::endl;
    ASSERT_TRUE(total_rows == 59986052);
}


TEST(TestWrite, MergeTreeWriteTest)
{
    auto config = local_engine::SerializedPlanParser::config;
    config->setString("s3.endpoint", "http://localhost:9000/tpch/");
    config->setString("s3.region", "us-east-1");
    config->setString("s3.access_key_id", "admin");
    config->setString("s3.secret_access_key", "password");
    auto global_context = local_engine::SerializedPlanParser::global_context;

    auto param = DB::MergeTreeData::MergingParams();
    auto settings = std::make_unique<DB::MergeTreeSettings>();
    settings->set("min_bytes_for_wide_part", Field(0));
    settings->set("min_rows_for_wide_part", Field(0));

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
    auto metadata = local_engine::buildMetaData(names_and_types_list, global_context);

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                           "tmp/test-write/",
                                                           *metadata,
                                                           false,
                                                           global_context,
                                                           "",
                                                           param,
                                                           std::move(settings)
    );

    auto files_info = std::make_shared<FilesInfo>();
    files_info->files = {"s3://tpch/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet"};
    auto source = std::make_shared<BatchParquetFileSource>(files_info, metadata->getSampleBlock(), SerializedPlanParser::global_context);

    QueryPipelineBuilder query_pipeline_builder;
    query_pipeline_builder.init(Pipe(source));
    query_pipeline_builder.setSinks([&](const Block &, Pipe::StreamType type) -> ProcessorPtr
                            {
                                if (type != Pipe::StreamType::Main)
                                    return nullptr;

                                return std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);
                            });
    auto executor = query_pipeline_builder.execute();
    executor->execute(1);
}
