#include <gtest/gtest.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>
#include <Parser/SparkColumnToCHColumn.h>
#include <Parser/CHColumnToSparkRow.h>
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
//    DB::StorageID id("default", "test");
//    std::string relative_path = TEST_DATA(/data/mergetree);
//    DB::StorageInMemoryMetadata storage_in_memory_metadata;
//    auto shared = DB::Context::createShared();
//    auto global = DB::Context::createGlobal(shared.get());
//    auto merging_params = DB::MergeTreeData::MergingParams();
//    auto storage_setting = std::make_unique<DB::MergeTreeSettings>();

//    DB::MergeTreeData(id, relative_path, storage_in_memory_metadata, global, "", merging_params, std::move(storage_setting), false, false, nullptr);
//std::string uri = "hdfs://d-bdap-nn-0002.cebbank.com:8020/user/ke/11";
//const size_t begin_of_path = uri.find('/', uri.find("//") + 2);
//const String path_from_uri = uri.substr(begin_of_path);
//const String uri_without_path = uri.substr(0, begin_of_path);
//std::cout << path_from_uri <<std::endl << uri_without_path <<std::endl;
//String for_match = path_from_uri;
//String path_for_ls = "/";
//const size_t first_glob = for_match.find_first_of("*?{");
//
//const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
//const String suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'
//const String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'
//std::cout << suffix_with_globs <<std::endl << prefix_without_globs;
}

int main(int argc, char **argv)
{
    dbms::SerializedPlanParser::initFunctionEnv();
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
