#include <gtest/gtest.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include "Storages/CustomStorageMergeTree.h"
#include <DataTypes/DataTypesNumber.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>
#include <Parser/SparkColumnToCHColumn.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Columns/ColumnVector.h>
#include <Parsers/ASTFunction.h>
#include <local/LocalServer.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/TableJoin.h>
#include <Storages/SelectQueryInfo.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/CustomMergeTreeSink.h>
//#include <Poco/URI.h>

using namespace dbms;

TEST(TestSelect, ReadRel)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto* schema = schema_builder
        .column("sepal_length", "FP64")
        .column("sepal_width", "FP64")
        .column("petal_length", "FP64")
        .column("petal_width", "FP64")
        .column("type", "I64").column("type_string", "String")
        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.read(  TEST_DATA(/data/iris.parquet), std::move(schema)).build();

    std::ofstream output;
    output.open(TEST_DATA(/../java/src/test/resources/plan.txt), std::fstream::in | std::fstream::out | std::fstream::trunc);
    //    output << plan->SerializeAsString();
    plan->SerializeToOstream(&output);
    output.flush();
    output.close();

    ASSERT_TRUE(plan->relations(0).root().input().has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    std::cout << "start execute" <<std::endl;
    dbms::LocalExecutor local_executor;
    dbms::SerializedPlanParser parser(dbms::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        std::cout << "fetch batch" << std::endl;
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkColumnToCHColumn converter;
        auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, ReadDate)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto* schema = schema_builder
                        .column("date", "Date")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.read(TEST_DATA(/data/date.parquet), std::move(schema)).build();

    ASSERT_TRUE(plan->relations(0).root().input().has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    std::cout << "start execute" <<std::endl;
    dbms::LocalExecutor local_executor;
    dbms::SerializedPlanParser parser(dbms::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        std::cout << "fetch batch" << std::endl;
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkColumnToCHColumn converter;
        auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

bool inside_main=true;
TEST(TestSelect, TestFilter)
{
    dbms::SerializedSchemaBuilder schema_builder;
    // sorted by key
    auto* schema = schema_builder
                        .column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64").column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    // sepal_length * 0.8
    auto * mul_exp = dbms::scalarFunction(dbms::MULTIPLY,
                                             {dbms::selection(2),
                                              dbms::literal(0.8)});
    // sepal_length * 0.8 < 4.0
    auto * less_exp = dbms::scalarFunction(dbms::LESS_THAN, {
                                                           mul_exp,
                                                           dbms::literal(4.0)
                                                                               });
    // type_string = '类型1'
    auto * type_0 = dbms::scalarFunction(dbms::EQUAL_TO, {dbms::selection(5),
                                                          dbms::literal("类型1")});

    auto * filter = dbms::scalarFunction(dbms::AND, {less_exp, type_0});
    auto plan = plan_builder
                    .registerSupportedFunctions()
                    .filter(filter)
                    .read(TEST_DATA(/data/iris.parquet), std::move(schema)).build();
//    ASSERT_TRUE(plan->relations(0).has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    std::cout << "start execute" <<std::endl;
    dbms::LocalExecutor local_executor;
    dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        std::cout << "fetch batch" << std::endl;
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_EQ(spark_row_info->getNumRows(), 1);
        local_engine::SparkColumnToCHColumn converter;
        auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, TestAgg)
{
    dbms::SerializedSchemaBuilder schema_builder;
    // sorted by key
    auto* schema = schema_builder
                        .column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64").column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto * mul_exp = dbms::scalarFunction(dbms::MULTIPLY,
                                          {dbms::selection(2),
                                           dbms::literal(0.8)});
    auto * less_exp = dbms::scalarFunction(dbms::LESS_THAN, {
                                                                mul_exp,
                                                                dbms::literal(4.0)
                                                            });
    auto * mul_exp2 = dbms::scalarFunction(dbms::MULTIPLY,
                                          {dbms::selection(2),
                                           dbms::literal(1.1)});
    auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
    auto plan = plan_builder
                    .registerSupportedFunctions()
                    .aggregate({}, {measure})
                    .filter(less_exp)
                    .read(TEST_DATA(/data/iris.parquet), std::move(schema)).build();
    //    ASSERT_TRUE(plan->relations(0).has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    std::cout << "start execute" <<std::endl;
    dbms::LocalExecutor local_executor;
    dbms::SerializedPlanParser parser(SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        std::cout << "fetch batch" << std::endl;
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_EQ(spark_row_info->getNumRows(), 1);
        ASSERT_EQ(spark_row_info->getNumCols(), 1);
        local_engine::SparkColumnToCHColumn converter;
        auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
        auto reader = SparkRowReader(spark_row_info->getNumCols());
        reader.pointTo(reinterpret_cast<int64_t>(spark_row_info->getBufferAddress() + spark_row_info->getOffsets()[1]), spark_row_info->getLengths()[0]);
        ASSERT_EQ(reader.getDouble(0), 103.2);
    }
}

TEST(TestSelect, MergeTreeWriteTest)
{
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto shared_context = Context::createShared();
    DB::LocalServer localServer;
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

    local_engine::CustomStorageMergeTree custom_merge_tree(DB::StorageID("default", "test"),
                                                                                    "test-intel/",
                                                                                    *metadata,
                                                                                    false,
                                                           global_context,
                                                                                    "",
                                                                                    param,
                                                                                    std::move(settings)
                                                                                );

    auto sink = std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);

    auto files_info = std::make_shared<FilesInfo>();
    files_info->files.push_back("/home/kyligence/Documents/test-dataset/intel-gazelle-test-150.snappy.parquet");
    auto source = std::make_shared<BatchParquetFileSource>(files_info, sink->getPort().getHeader());

    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
    QueryPipeline query_pipeline;
    query_pipeline.init(Pipe(source));
    query_pipeline.setSinks([&](const Block &, QueryPipeline::StreamType type) -> ProcessorPtr
                          {
                              if (type != QueryPipeline::StreamType::Main)
                                  return nullptr;

                              return std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);
                          });
    query_pipeline.execute()->execute(1);
}

int main(int argc, char **argv)
{
    dbms::SerializedPlanParser::initFunctionEnv();
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
