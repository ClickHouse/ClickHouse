#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/DebugUtils.h>
#include <gtest/gtest.h>

TEST(TestFuntion, hash)
{
    auto & factory = DB::FunctionFactory::instance();
    auto function = factory.get("murmurHash2_64", local_engine::SerializedPlanParser::global_context);
    auto type0 = DataTypeFactory::instance().get("String");
    auto column0 = type0->createColumn();
    column0->insert("A");
    column0->insert("A");
    column0->insert("B");
    column0->insert("c");

    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    ColumnsWithTypeAndName columns = {ColumnWithTypeAndName(std::move(column0),type0, "string0"),
                                      ColumnWithTypeAndName(std::move(column1),type0, "string0")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(0), result->getUInt(1));
}


//int main(int argc, char ** argv)
//{
//    SharedContextHolder shared_context = Context::createShared();
//    local_engine::SerializedPlanParser::global_context = Context::createGlobal(shared_context.get());
//    local_engine::SerializedPlanParser::global_context->makeGlobalContext();
//    local_engine::SerializedPlanParser::initFunctionEnv();
//    ::testing::InitGoogleTest(&argc, argv);
//    return RUN_ALL_TESTS();
//}
