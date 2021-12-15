#include <gtest/gtest.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>
#include <Parser/SparkColumnToCHColumn.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Poco/URI.h>


TEST(TestSelect, ReadRel)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto schema = schema_builder
        .column("sepal_length", "FP64")
        .column("sepal_width", "FP64")
        .column("petal_length", "FP64")
        .column("petal_width", "FP64")
        .column("type", "I64").column("type_string", "String")
        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.files(  TEST_DATA(/data/iris.parquet), std::move(schema)).build();

    std::ofstream output;
    output.open(TEST_DATA(/../java/src/test/resources/plan.txt), std::fstream::in | std::fstream::out | std::fstream::trunc);
    //    output << plan->SerializeAsString();
    plan->SerializeToOstream(&output);
    output.flush();
    output.close();

    ASSERT_TRUE(plan->relations(0).has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    std::cout << "start execute" <<std::endl;
    dbms::LocalExecutor local_executor;
    auto query_plan = dbms::SerializedPlanParser::parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        std::cout << "fetch batch" << std::endl;
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkColumnToCHColumn converter;
        auto block = converter.convertCHColumnToSparkRow(*spark_row_info, local_executor.getHeader());
        ASSERT_GT(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, PerformanceTest)
{

    Stopwatch stopwatch;
    stopwatch.start();
    for (int i=0; i < 10; i++)
    {
        dbms::SerializedSchemaBuilder schema_builder;
        auto schema = schema_builder
                          .column("l_orderkey", "I64")
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
        auto plan = plan_builder.files("/home/kyligence/Documents/intel-gazelle-test.snappy.parquet", std::move(schema)).build();

        ASSERT_TRUE(plan->relations(0).has_read());
        ASSERT_EQ(plan->relations_size(), 1);
        auto query_plan = dbms::SerializedPlanParser::parse(std::move(plan));
        std::cout << "start execute" << std::endl;
        dbms::LocalExecutor local_executor;

        local_executor.execute(std::move(query_plan));
        ASSERT_TRUE(local_executor.hasNext());
        while (local_executor.hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
            ASSERT_GT(spark_row_info->getNumRows(), 0);
            std::cout << "fetch batch" << spark_row_info->getNumRows() << " rows"
                      << ""
                      << "" << std::endl;
        }
    }
    auto duration = stopwatch.elapsedMilliseconds();
    std::cout <<"duration:" << duration << std::endl;
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

    Poco::URI uri("hdfs://clusterB/test.txt");
    std::cout << uri.toString() << std::endl;
    ASSERT_GT("clusterB", uri.getHost());

//    DB::MergeTreeData(id, relative_path, storage_in_memory_metadata, global, "", merging_params, std::move(storage_setting), false, false, nullptr);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
