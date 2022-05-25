#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/BatchParquetFileSource.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Common/MergeTreeTool.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinSwitcher.h>


using namespace DB;
using namespace local_engine;

TEST(TestJoin, simple)
{
    auto global_context = SerializedPlanParser::global_context;
    auto & factory = DB::FunctionFactory::instance();
    auto function = factory.get("murmurHash2_64", local_engine::SerializedPlanParser::global_context);
    auto int_type = DataTypeFactory::instance().get("Int32");
    auto column0 = int_type->createColumn();
    column0->insert(1);
    column0->insert(2);
    column0->insert(3);
    column0->insert(4);

    auto column1 = int_type->createColumn();
    column1->insert(2);
    column1->insert(4);
    column1->insert(6);
    column1->insert(8);

    ColumnsWithTypeAndName columns = {ColumnWithTypeAndName(std::move(column0),int_type, "colA"),
                                      ColumnWithTypeAndName(std::move(column1),int_type, "colB")};
    Block left(columns);

    auto column3 = int_type->createColumn();
    column3->insert(1);
    column3->insert(2);
    column3->insert(3);
    column3->insert(5);

    auto column4 = int_type->createColumn();
    column4->insert(1);
    column4->insert(3);
    column4->insert(5);
    column4->insert(9);

    ColumnsWithTypeAndName columns2 = {ColumnWithTypeAndName(std::move(column3),int_type, "colD"),
                                      ColumnWithTypeAndName(std::move(column4),int_type, "colC")};
    Block right(columns2);

    auto left_table = std::make_shared<SourceFromSingleChunk>(left);
    auto right_table = std::make_shared<SourceFromSingleChunk>(right);
    QueryPlan left_plan;
    left_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(left_table)));
    QueryPlan right_plan;
    right_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(right_table)));

    auto join = std::make_shared<TableJoin>(global_context->getSettings(), global_context->getTemporaryVolume());
    join->setKind(ASTTableJoin::Kind::Inner);
    join->setStrictness(ASTTableJoin::Strictness::All);
    join->addDisjunct();
    auto required_rkey = NameAndTypePair("colD", int_type);
    join->addJoinedColumn(required_rkey);
    join->addJoinedColumn(NameAndTypePair("colC", int_type));
    ASTPtr lkey = std::make_shared<ASTIdentifier>("colA");
    ASTPtr rkey = std::make_shared<ASTIdentifier>("colD");
    join->addOnKeys(lkey, rkey);


    auto hash_join = std::make_shared<HashJoin>(join, right);

    QueryPlanStepPtr join_step = std::make_unique<JoinStep>(
        left_plan.getCurrentDataStream(),
        right_plan.getCurrentDataStream(),
        hash_join,
        8192);

    join_step->setStepDescription("JOIN");


    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

    auto query_plan = QueryPlan();
    query_plan.unitePlans(std::move(join_step), {std::move(plans)});

    ActionsDAGPtr project = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getNamesAndTypesList());
    project->project({NameWithAlias("colA", "colA"),NameWithAlias("colB", "colB"),NameWithAlias("colD", "colD"),NameWithAlias("colC", "colC")});
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), project);
    query_plan.addStep(std::move(project_step));
    auto pipeline = query_plan.buildQueryPipeline(QueryPlanOptimizationSettings(), BuildQueryPipelineSettings());
    auto executable_pipe = QueryPipelineBuilder::getPipeline(std::move(*pipeline));
    PullingPipelineExecutor executor(executable_pipe);
    auto res = pipeline->getHeader().cloneEmpty();
    executor.pull(res);
    debug::headBlock(res);
}
