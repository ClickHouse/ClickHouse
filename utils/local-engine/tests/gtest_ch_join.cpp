#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/BatchParquetFileSource.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Common/MergeTreeTool.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageJoinFromReadBuffer.h>

#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>


using namespace DB;
using namespace local_engine;

TEST(TestJoin, simple)
{
    auto global_context = SerializedPlanParser::global_context;
    local_engine::SerializedPlanParser::global_context->setSetting("join_use_nulls", true);
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
    join->setKind(ASTTableJoin::Kind::Left);
    join->setStrictness(ASTTableJoin::Strictness::All);
    join->setColumnsFromJoinedTable(right.getNamesAndTypesList());
    join->addDisjunct();
    ASTPtr lkey = std::make_shared<ASTIdentifier>("colA");
    ASTPtr rkey = std::make_shared<ASTIdentifier>("colD");
    join->addOnKeys(lkey, rkey);
    for (const auto & column : join->columnsFromJoinedTable())
    {
        join->addJoinedColumn(column);
    }

    auto left_keys = left.getNamesAndTypesList();
    join->addJoinedColumnsAndCorrectTypes(left_keys, true);
    std::cerr << "after join:\n";
    for (const auto& key : left_keys)
    {
        std::cerr << key.dump() <<std::endl;
    }
    ActionsDAGPtr left_convert_actions = nullptr;
    ActionsDAGPtr right_convert_actions = nullptr;
    std::tie(left_convert_actions, right_convert_actions) = join->createConvertingActions(left.getColumnsWithTypeAndName(), right.getColumnsWithTypeAndName());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right_plan.getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        right_plan.addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right_plan.getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        left_plan.addStep(std::move(converting_step));
    }
    auto hash_join = std::make_shared<HashJoin>(join, right_plan.getCurrentDataStream().header);

    QueryPlanStepPtr join_step = std::make_unique<JoinStep>(
        left_plan.getCurrentDataStream(),
        right_plan.getCurrentDataStream(),
        hash_join,
        8192);

    std::cerr<< "join step:" <<join_step->getOutputStream().header.dumpStructure() << std::endl;

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

    auto query_plan = QueryPlan();
    query_plan.unitePlans(std::move(join_step), {std::move(plans)});
    std::cerr << query_plan.getCurrentDataStream().header.dumpStructure() << std::endl;
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


TEST(TestJoin, StorageJoinFromReadBufferTest)
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
    std::string buf;
    WriteBufferFromString write_buf(buf);
    NativeWriter writer(write_buf, 0, right.cloneEmpty());
    writer.write(right);

    auto in = std::make_unique<ReadBufferFromString>(buf);
    auto metadata = local_engine::buildMetaData(right.getNamesAndTypesList(), global_context);

    auto join_storage = std::shared_ptr<StorageJoinFromReadBuffer>(new StorageJoinFromReadBuffer(std::move(in), StorageID("default", "test"), {"colD"}, false, SizeLimits(), ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All, ColumnsDescription(right.getNamesAndTypesList()), ConstraintsDescription(), "test", true));
    auto storage_snapshot = std::make_shared<StorageSnapshot>(*join_storage, metadata);
    auto left_table = std::make_shared<SourceFromSingleChunk>(left);
    SelectQueryInfo query_info;
    auto right_table = join_storage->read(right.getNames(), storage_snapshot, query_info, global_context, QueryProcessingStage::Enum::FetchColumns, 8192, 1);
    QueryPlan left_plan;
    left_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(left_table)));

    auto join = std::make_shared<TableJoin>(SizeLimits(), false, ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All, right.getNames());
    auto required_rkey = NameAndTypePair("colD", int_type);
    join->addJoinedColumn(required_rkey);
    join->addJoinedColumn(NameAndTypePair("colC", int_type));
    ASTPtr lkey = std::make_shared<ASTIdentifier>("colA");
    ASTPtr rkey = std::make_shared<ASTIdentifier>("colD");
    join->addOnKeys(lkey, rkey);


    auto hash_join = join_storage->getJoinLocked(join, global_context);

    QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(
        left_plan.getCurrentDataStream(),
        hash_join,
        8192);

    join_step->setStepDescription("JOIN");
    left_plan.addStep(std::move(join_step));

    ActionsDAGPtr project = std::make_shared<ActionsDAG>(left_plan.getCurrentDataStream().header.getNamesAndTypesList());
    project->project({NameWithAlias("colA", "colA"),NameWithAlias("colB", "colB"),NameWithAlias("colD", "colD"),NameWithAlias("colC", "colC")});
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), project);
    left_plan.addStep(std::move(project_step));
    auto pipeline = left_plan.buildQueryPipeline(QueryPlanOptimizationSettings(), BuildQueryPipelineSettings());
    auto executable_pipe = QueryPipelineBuilder::getPipeline(std::move(*pipeline));
    PullingPipelineExecutor executor(executable_pipe);
    auto res = pipeline->getHeader().cloneEmpty();
    executor.pull(res);
    debug::headBlock(res);
}

