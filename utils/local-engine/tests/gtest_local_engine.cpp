#include <fstream>
#include <iostream>
#include <Poco/Util/MapConfiguration.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/CustomStorageMergeTree.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include "testConfig.h"
#include "Common/Logger.h"
#include "Common/DebugUtils.h"

using namespace local_engine;
using namespace dbms;

TEST(TestSelect, ReadRel)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto * schema = schema_builder.column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64")
                        .column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.read(TEST_DATA(/data/iris.parquet), std::move(schema)).build();

    ASSERT_TRUE(plan->relations(0).root().input().has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, ReadDate)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto * schema = schema_builder.column("date", "Date").build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.read(TEST_DATA(/data/date.parquet), std::move(schema)).build();

    ASSERT_TRUE(plan->relations(0).root().input().has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

bool inside_main = true;
TEST(TestSelect, TestFilter)
{
    dbms::SerializedSchemaBuilder schema_builder;
    // sorted by key
    auto * schema = schema_builder.column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64")
                        .column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    // sepal_length * 0.8
    auto * mul_exp = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(2), dbms::literal(0.8)});
    // sepal_length * 0.8 < 4.0
    auto * less_exp = dbms::scalarFunction(dbms::LESS_THAN, {mul_exp, dbms::literal(4.0)});
    // type_string = '类型1'
    auto * type_0 = dbms::scalarFunction(dbms::EQUAL_TO, {dbms::selection(5), dbms::literal("类型1")});

    auto * filter = dbms::scalarFunction(dbms::AND, {less_exp, type_0});
    auto plan = plan_builder.registerSupportedFunctions().filter(filter).read(TEST_DATA(/data/iris.parquet), std::move(schema)).build();
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_EQ(spark_row_info->getNumRows(), 1);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, TestAgg)
{
    dbms::SerializedSchemaBuilder schema_builder;
    // sorted by key
    auto * schema = schema_builder.column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64")
                        .column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto * mul_exp = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(2), dbms::literal(0.8)});
    auto * less_exp = dbms::scalarFunction(dbms::LESS_THAN, {mul_exp, dbms::literal(4.0)});
    auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
    auto plan = plan_builder.registerSupportedFunctions()
                    .aggregate({}, {measure})
                    .filter(less_exp)
                    .read(TEST_DATA(/data/iris.parquet), std::move(schema))
                    .build();
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_EQ(spark_row_info->getNumRows(), 1);
        ASSERT_EQ(spark_row_info->getNumCols(), 1);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
        auto reader = SparkRowReader(block->getDataTypes());
        reader.pointTo(spark_row_info->getBufferAddress() + spark_row_info->getOffsets()[1], spark_row_info->getLengths()[0]);
        ASSERT_EQ(reader.getDouble(0), 103.2);
    }
}

TEST(TestSelect, MergeTreeWriteTest)
{
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setPath("/home/kyligence/Documents/clickhouse_conf/data/");
    global_context->getDisksMap().emplace();
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

    local_engine::CustomStorageMergeTree custom_merge_tree(
        DB::StorageID("default", "test"), "test-intel/", *metadata, false, global_context, "", param, std::move(settings));

    auto sink = std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);

    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file = files.add_items();
    std::string file_path = "file:///home/kyligence/Documents/test-dataset/intel-gazelle-test-150.snappy.parquet";
    file->set_uri_file(file_path);
    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file->mutable_parquet()->CopyFrom(parquet_format);
    auto source = std::make_shared<SubstraitFileSource>(SerializedPlanParser::global_context, metadata->getSampleBlock(), files);

    QueryPipelineBuilder query_pipeline;
    query_pipeline.init(Pipe(source));
    query_pipeline.setSinks(
        [&](const Block &, Pipe::StreamType type) -> ProcessorPtr
        {
            if (type != Pipe::StreamType::Main)
                return nullptr;

            return std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);
        });
    auto executor = query_pipeline.execute();
    executor->execute(1);
}

TEST(TESTUtil, TestByteToLong)
{
    Int64 expected = 0xf085460ccf7f0000l;
    char * arr = new char[8];
    arr[0] = -16;
    arr[1] = -123;
    arr[2] = 70;
    arr[3] = 12;
    arr[4] = -49;
    arr[5] = 127;
    arr[6] = 0;
    arr[7] = 0;
    std::reverse(arr, arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(arr)[0];
    std::cout << std::to_string(result);

    ASSERT_EQ(expected, result);
}


TEST(TestSimpleAgg, TestGenerate)
{
//    dbms::SerializedSchemaBuilder schema_builder;
//    auto * schema = schema_builder.column("l_orderkey", "I64")
//                        .column("l_partkey", "I64")
//                        .column("l_suppkey", "I64")
//                        .build();
//    dbms::SerializedPlanBuilder plan_builder;
//    auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(6)});
//    auto plan
//        = plan_builder.registerSupportedFunctions()
//              .aggregate({}, {measure})
//              .read(
//                  //"/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
//                  "/data0/tpch100_zhichao/parquet_origin/lineitem/part-00087-066b93b4-39e1-4d46-83ab-d7752096b599-c000.snappy.parquet",
//                  std::move(schema))
//              .build();
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
////    auto query_plan = parser.parse(std::move(plan));

    //std::ifstream t("/home/hongbin/develop/experiments/221011_substrait_agg_on_empty_table.json");
    //std::ifstream t("/home/hongbin/develop/experiments/221101_substrait_agg_on_simple_table_last_phrase.json");
    std::ifstream t("/home/hongbin/develop/experiments/221102_substrait_agg_and_countdistinct_second_phrase.json");
    std::string str((std::istreambuf_iterator<char>(t)),
                    std::istreambuf_iterator<char>());
    auto query_plan = parser.parseJson(str);
    local_engine::LocalExecutor local_executor;
    local_executor.execute(std::move(query_plan));
    while (local_executor.hasNext())
    {
        //local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();

        auto block = local_executor.nextColumnar();
        debug::headBlock(*block);
    }
}

TEST(TestSubstrait, TestGenerate)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto * schema = schema_builder.column("l_discount", "FP64")
                        .column("l_extendedprice", "FP64")
                        .column("l_quantity", "FP64")
                        .column("l_shipdate_new", "Date")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto * agg_mul = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(1), dbms::selection(0)});
    auto * measure1 = dbms::measureFunction(dbms::SUM, {agg_mul});
    auto * measure2 = dbms::measureFunction(dbms::SUM, {dbms::selection(1)});
    auto * measure3 = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
    auto plan
        = plan_builder.registerSupportedFunctions()
              .aggregate({}, {measure1, measure2, measure3})
              .project({dbms::selection(2), dbms::selection(1), dbms::selection(0)})
              .filter(dbms::scalarFunction(
                  dbms::AND,
                  {dbms::scalarFunction(
                       AND,
                       {dbms::scalarFunction(
                            AND,
                            {dbms::scalarFunction(
                                 AND,
                                 {dbms::scalarFunction(
                                      AND,
                                      {dbms::scalarFunction(
                                           AND,
                                           {dbms::scalarFunction(
                                                AND,
                                                {scalarFunction(IS_NOT_NULL, {selection(3)}), scalarFunction(IS_NOT_NULL, {selection(0)})}),
                                            scalarFunction(IS_NOT_NULL, {selection(2)})}),
                                       dbms::scalarFunction(GREATER_THAN_OR_EQUAL, {selection(3), literalDate(8766)})}),
                                  scalarFunction(LESS_THAN, {selection(3), literalDate(9131)})}),
                             scalarFunction(GREATER_THAN_OR_EQUAL, {selection(0), literal(0.05)})}),
                        scalarFunction(LESS_THAN_OR_EQUAL, {selection(0), literal(0.07)})}),
                   scalarFunction(LESS_THAN, {selection(2), literal(24.0)})}))
              .readMergeTree("default", "test", "usr/code/data/test-mergetree", 1, 12, std::move(schema))
              .build();
    std::ofstream output;
    output.open("/home/kyligence/Documents/code/ClickHouse/plan.txt", std::fstream::in | std::fstream::out | std::fstream::trunc);
    output << plan->SerializeAsString();
    //    plan->SerializeToOstream(&output);
    output.flush();
    output.close();
}

TEST(ReadBufferFromFile, seekBackwards)
{
    static constexpr size_t N = 256;
    static constexpr size_t BUF_SIZE = 64;

    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        for (size_t i = 0; i < N; ++i)
            writeIntBinary(i, out);
    }

    ReadBufferFromFile in(tmp_file->path(), BUF_SIZE);
    size_t x;

    /// Read something to initialize the buffer.
    in.seek(BUF_SIZE * 10, SEEK_SET);
    readIntBinary(x, in);

    /// Check 2 consecutive  seek calls without reading.
    in.seek(BUF_SIZE * 2, SEEK_SET);
    //    readIntBinary(x, in);
    in.seek(BUF_SIZE, SEEK_SET);

    readIntBinary(x, in);
    ASSERT_EQ(x, 8);
}

int main(int argc, char ** argv)
{
    local_engine::Logger::initConsoleLogger();

    SharedContextHolder shared_context = Context::createShared();
    local_engine::SerializedPlanParser::global_context = Context::createGlobal(shared_context.get());
    local_engine::SerializedPlanParser::global_context->makeGlobalContext();
    auto config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    local_engine::SerializedPlanParser::global_context->setConfig(config);
    local_engine::SerializedPlanParser::global_context->setPath("/tmp");
    local_engine::SerializedPlanParser::global_context->getDisksMap().emplace();
    local_engine::SerializedPlanParser::initFunctionEnv();
    auto & factory = local_engine::ReadBufferBuilderFactory::instance();
    registerReadBufferBuildes(factory);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
