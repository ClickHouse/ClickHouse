#include <gtest/gtest.h>
#include <Parser/SerializedPlanParser.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <iostream>
#include "testConfig.h"
#include <fstream>

TEST(TestSelect, ReadRel)
{
    dbms::SerializedSchemaBuilder schema_builder;
    auto schema = schema_builder
        .column("sepal_length", "FP64")
        .column("sepal_width", "FP64")
        .column("petal_length", "FP64")
        .column("petal_width", "FP64")
        .column("type", "I64")
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
    auto query_plan = dbms::SerializedPlanParser::parse(std::move(plan));
    std::cout << "start execute" <<std::endl;
    dbms::LocalExecutor local_executor;
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while(local_executor.hasNext())
    {
        std::cout << "fetch batch" <<std::endl;
        std::string arrow_chunk = local_executor.next();
        ASSERT_GT(arrow_chunk.size(), 0);
    }
}

TEST(TestSelect, MergeTreeWriteTest)
{
    DB::StorageID id("default", "test");
    std::string relative_path = TEST_DATA(/data/mergetree);
    DB::StorageInMemoryMetadata storage_in_memory_metadata;
    auto shared = DB::Context::createShared();
    auto global = DB::Context::createGlobal(shared.get());
    auto merging_params = DB::MergeTreeData::MergingParams();
    auto storage_setting = std::make_unique<DB::MergeTreeSettings>();

//    DB::MergeTreeData(id, relative_path, storage_in_memory_metadata, global, "", merging_params, std::move(storage_setting), false, false, nullptr);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}