#include <base/logger_useful.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Builder/BroadCastJoinBuilder.h>
#include <Columns/ColumnSet.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Operator/PartitionColumnFillingTransform.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBlockOutputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/DebugUtils.h>
#include <Common/JoinHelper.h>
#include <Common/MergeTreeTool.h>
#include <Common/StringUtils.h>
#include <google/protobuf/util/json_util.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>

#include "SerializedPlanParser.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int UNKNOWN_FUNCTION;
}
}

namespace local_engine
{
using namespace DB;

void join(ActionsDAG::NodeRawConstPtrs v, char c, std::string & s)
{
    s.clear();
    for (auto p = v.begin(); p != v.end(); ++p)
    {
        s += (*p)->result_name;
        if (p != v.end() - 1)
            s += c;
    }
}

std::string typeName(const substrait::Type & type)
{
    if (type.has_string())
    {
        return "String";
    }
    else if (type.has_i8())
    {
        return "I8";
    }
    else if (type.has_i16())
    {
        return "I16";
    }
    else if (type.has_i32())
    {
        return "I32";
    }
    else if (type.has_i64())
    {
        return "I64";
    }
    else if (type.has_fp32())
    {
        return "FP32";
    }
    else if (type.has_fp64())
    {
        return "FP64";
    }
    else if (type.has_bool_())
    {
        return "Boolean";
    }
    else if (type.has_date())
    {
        return "Date";
    }
    else if (type.has_timestamp())
    {
        return "Timestamp";
    }

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "unknown type {}", magic_enum::enum_name(type.kind_case()));
}

bool isTypeSame(const substrait::Type & type, DataTypePtr data_type)
{
    static const std::map<std::string, std::string> type_mapping
        = {{"I8", "Int8"},
           {"I16", "Int16"},
           {"I32", "Int32"},
           {"I64", "Int64"},
           {"FP32", "Float32"},
           {"FP64", "Float64"},
           {"Date", "Date32"},
           {"Timestamp", "DateTime64(6)"},
           {"String", "String"},
           {"Boolean", "UInt8"}};

    std::string type_name = typeName(type);
    if (!type_mapping.contains(type_name))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", type_name);

    return type_mapping.at(type_name) == data_type->getName();
}

std::string getCastFunction(const substrait::Type & type)
{
    std::string ch_function_name;
    if (type.has_fp64())
    {
        ch_function_name = "toFloat64";
    }
    else if (type.has_fp32())
    {
        ch_function_name = "toFloat32";
    }
    else if (type.has_string())
    {
        ch_function_name = "toString";
    }
    else if (type.has_i64())
    {
        ch_function_name = "toInt64";
    }
    else if (type.has_i32())
    {
        ch_function_name = "toInt32";
    }
    else if (type.has_i16())
    {
        ch_function_name = "toInt16";
    }
    else if (type.has_i8())
    {
        ch_function_name = "toInt8";
    }
    else if (type.has_date())
    {
        ch_function_name = "toDate32";
    }
    // TODO need complete param: scale
    else if (type.has_timestamp())
    {
        ch_function_name = "toDateTime64";
    }
    else if (type.has_bool_())
    {
        ch_function_name = "toUInt8";
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support cast type {}", type.DebugString());

    return ch_function_name;
}

bool SerializedPlanParser::isReadRelFromJava(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    return rel.local_files().items().size() == 1 && rel.local_files().items().at(0).uri_file().starts_with("iterator");
}

QueryPlanPtr SerializedPlanParser::parseReadRealWithLocalFile(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    auto header = parseNameStruct(rel.base_schema());
    auto source = std::make_shared<SubstraitFileSource>(context, header, rel.local_files());
    auto source_pipe = Pipe(source);
    auto source_step = std::make_unique<ReadFromStorageStep>(std::move(source_pipe), "substrait local files");
    source_step->setStepDescription("read local files");
    auto query_plan = std::make_unique<QueryPlan>();
    query_plan->addStep(std::move(source_step));
    return query_plan;
}

QueryPlanPtr SerializedPlanParser::parseReadRealWithJavaIter(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.local_files().items().size() == 1);
    assert(rel.has_base_schema());
    auto iter = rel.local_files().items().at(0).uri_file();
    auto pos = iter.find(':');
    auto iter_index = std::stoi(iter.substr(pos + 1, iter.size()));
    auto plan = std::make_unique<QueryPlan>();
    auto source = std::make_shared<SourceFromJavaIter>(parseNameStruct(rel.base_schema()), input_iters[iter_index]);
    QueryPlanStepPtr source_step = std::make_unique<ReadFromPreparedSource>(Pipe(source), context);
    source_step->setStepDescription("Read From Java Iter");
    plan->addStep(std::move(source_step));
    return plan;
}

QueryPlanPtr SerializedPlanParser::parseMergeTreeTable(const substrait::ReadRel & rel)
{
    Stopwatch watch;
    watch.start();
    assert(rel.has_extension_table());
    assert(rel.has_base_schema());
    std::string table = rel.extension_table().detail().value();
    auto merge_tree_table = local_engine::parseMergeTreeTableString(table);
    auto header = parseNameStruct(rel.base_schema());
    auto names_and_types_list = header.getNamesAndTypesList();
    auto storage_factory = StorageMergeTreeFactory::instance();
    auto metadata = buildMetaData(names_and_types_list, this->context);
    auto t_metadata = watch.elapsedMicroseconds();
    query_context.metadata = metadata;
    auto storage = storage_factory.getStorage(
        StorageID(merge_tree_table.database, merge_tree_table.table),
        metadata->getColumns(),
        [merge_tree_table, metadata]() -> CustomStorageMergeTreePtr
        {
            auto custom_storage_merge_tree = std::make_shared<CustomStorageMergeTree>(
                StorageID(merge_tree_table.database, merge_tree_table.table),
                merge_tree_table.relative_path,
                *metadata,
                false,
                global_context,
                "",
                MergeTreeData::MergingParams(),
                buildMergeTreeSettings());
            custom_storage_merge_tree->loadDataParts(false);
            return custom_storage_merge_tree;
        });
    query_context.storage_snapshot = std::make_shared<StorageSnapshot>(*storage, metadata);
    auto t_storage = watch.elapsedMicroseconds() - t_metadata;
    query_context.custom_storage_merge_tree = storage;
    auto query_info = buildQueryInfo(names_and_types_list);
    auto data_parts = query_context.custom_storage_merge_tree->getDataPartsVector();
    int min_block = merge_tree_table.min_block;
    int max_block = merge_tree_table.max_block;
    MergeTreeData::DataPartsVector selected_parts;
    std::copy_if(
        std::begin(data_parts),
        std::end(data_parts),
        std::inserter(selected_parts, std::begin(selected_parts)),
        [min_block, max_block](MergeTreeData::DataPartPtr part)
        { return part->info.min_block >= min_block && part->info.max_block < max_block; });
    if (selected_parts.empty())
    {
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "part {} to {} not found.", min_block, max_block);
    }
    auto query = query_context.custom_storage_merge_tree->reader.readFromParts(
        selected_parts, names_and_types_list.getNames(), query_context.storage_snapshot, *query_info, this->context, 4096 * 2, 1);
    auto t_pipe = watch.elapsedMicroseconds() - t_storage;
    watch.stop();
    LOG_TRACE(
        &Poco::Logger::get("SerializedPlanParser"),
        "get metadata {} ms; get storage {} ms; get pipe {} ms",
        t_metadata / 1000.0,
        t_storage / 1000.0,
        t_pipe / 1000.0);
    return query;
}

Block SerializedPlanParser::parseNameStruct(const substrait::NamedStruct & struct_)
{
    auto internal_cols = std::make_unique<std::vector<ColumnWithTypeAndName>>();
    internal_cols->reserve(struct_.names_size());
    for (int i = 0; i < struct_.names_size(); ++i)
    {
        const auto & name = struct_.names(i);
        const auto & type = struct_.struct_().types(i);
        auto data_type = parseType(type);
        Poco::StringTokenizer name_parts(name, "#");
        if (name_parts.count() == 4)
        {
            auto agg_function_name = name_parts[3];
            AggregateFunctionProperties properties;
            auto tmp = AggregateFunctionFactory::instance().get(name_parts[3], {data_type}, {}, properties);
            data_type = tmp->getStateType();
        }
        internal_cols->push_back(ColumnWithTypeAndName(data_type, name));
    }
    return Block(*std::move(internal_cols));
}

static DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type)
{
    if (nullable == substrait::Type_Nullability_NULLABILITY_NULLABLE)
    {
        return std::make_shared<DataTypeNullable>(nested_type);
    }
    else
    {
        return nested_type;
    }
}

DataTypePtr SerializedPlanParser::parseType(const substrait::Type & type)
{
    DataTypePtr internal_type = nullptr;
    auto & factory = DataTypeFactory::instance();
    if (type.has_bool_())
    {
        internal_type = factory.get("UInt8");
        internal_type = wrapNullableType(type.bool_().nullability(), internal_type);
    }
    else if (type.has_i8())
    {
        internal_type = factory.get("Int8");
        internal_type = wrapNullableType(type.i8().nullability(), internal_type);
    }
    else if (type.has_i16())
    {
        internal_type = factory.get("Int16");
        internal_type = wrapNullableType(type.i16().nullability(), internal_type);
    }
    else if (type.has_i32())
    {
        internal_type = factory.get("Int32");
        internal_type = wrapNullableType(type.i32().nullability(), internal_type);
    }
    else if (type.has_i64())
    {
        internal_type = factory.get("Int64");
        internal_type = wrapNullableType(type.i64().nullability(), internal_type);
    }
    else if (type.has_string())
    {
        internal_type = factory.get("String");
        internal_type = wrapNullableType(type.string().nullability(), internal_type);
    }
    else if (type.has_fp32())
    {
        internal_type = factory.get("Float32");
        internal_type = wrapNullableType(type.fp32().nullability(), internal_type);
    }
    else if (type.has_fp64())
    {
        internal_type = factory.get("Float64");
        internal_type = wrapNullableType(type.fp64().nullability(), internal_type);
    }
    else if (type.has_date())
    {
        internal_type = factory.get("Date32");
        internal_type = wrapNullableType(type.date().nullability(), internal_type);
    }
    else if (type.has_timestamp())
    {
        internal_type = factory.get("DateTime64(6)");
        internal_type = wrapNullableType(type.timestamp().nullability(), internal_type);
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support type {}", type.DebugString());
    }
    return internal_type;
}
QueryPlanPtr SerializedPlanParser::parse(std::unique_ptr<substrait::Plan> plan)
{
    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    if (logger->debug())
    {
        namespace pb_util = google::protobuf::util;
        pb_util::JsonOptions options;
        std::string json;
        pb_util::MessageToJsonString(*plan, &json, options);
        LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "substrait plan:{}", json);
    }
    if (plan->extensions_size() > 0)
    {
        for (const auto & extension : plan->extensions())
        {
            if (extension.has_extension_function())
            {
                this->function_mapping.emplace(
                    std::to_string(extension.extension_function().function_anchor()), extension.extension_function().name());
            }
        }
    }
    if (plan->relations_size() == 1)
    {
        auto root_rel = plan->relations().at(0);
        if (!root_rel.has_root())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "must have root rel!");
        }

        auto query_plan = parseOp(root_rel.root().input());
        ActionsDAGPtr actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
        NamesWithAliases aliases;
        auto cols = query_plan->getCurrentDataStream().header.getNamesAndTypesList();
        for (int i = 0; i < root_rel.root().names_size(); i++)
        {
            aliases.emplace_back(NameWithAlias(cols.getNames()[i], root_rel.root().names(i)));
        }
        actions_dag->project(aliases);
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
        expression_step->setStepDescription("Rename Output");
        query_plan->addStep(std::move(expression_step));

        if (logger->trace())
        {
            WriteBufferFromOwnString plan_string;
            QueryPlan::ExplainPlanOptions options;
            options.header = true;
            query_plan->explainPlan(plan_string, options);
            LOG_TRACE(
                &Poco::Logger::get("SerializedPlanParser"),
                "pipeline \n{}\n,pipeline output:\n{}",
                plan_string.str(),
                query_plan->getCurrentDataStream().header.dumpStructure());
        }

        return query_plan;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "too many relations found");
    }
}

QueryPlanPtr SerializedPlanParser::parseOp(const substrait::Rel & rel)
{
    QueryPlanPtr query_plan;
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kFetch: {
            const auto & limit = rel.fetch();
            query_plan = parseOp(limit.input());
            auto limit_step = std::make_unique<LimitStep>(query_plan->getCurrentDataStream(), limit.count(), limit.offset());
            query_plan->addStep(std::move(limit_step));
            break;
        }
        case substrait::Rel::RelTypeCase::kFilter: {
            const auto & filter = rel.filter();
            query_plan = parseOp(filter.input());
            std::string filter_name;
            std::vector<String> required_columns;
            auto actions_dag
                = parseFunction(query_plan->getCurrentDataStream(), filter.condition(), filter_name, required_columns, nullptr, true);
            auto input = query_plan->getCurrentDataStream().header.getNames();
            Names input_with_condition(input);
            input_with_condition.emplace_back(filter_name);
            actions_dag->removeUnusedActions(input_with_condition);
            auto filter_step = std::make_unique<FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
            query_plan->addStep(std::move(filter_step));

            // remove nullable
            if (!required_columns.empty())
            {
                auto remove_nullable_actions_dag
                    = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
                removeNullable(required_columns, remove_nullable_actions_dag);
                auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), remove_nullable_actions_dag);
                expression_step->setStepDescription("Remove nullable properties");
                query_plan->addStep(std::move(expression_step));
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kProject: {
            const auto & project = rel.project();
            query_plan = parseOp(project.input());
            const auto & expressions = project.expressions();
            auto actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
            NamesWithAliases required_columns;
            std::set<String> distinct_columns;

            for (const auto & expr : expressions)
            {
                if (expr.has_selection())
                {
                    const auto * field = actions_dag->getInputs()[expr.selection().direct_reference().struct_field().field()];
                    if (distinct_columns.contains(field->result_name))
                    {
                        auto unique_name = getUniqueName(field->result_name);
                        required_columns.emplace_back(NameWithAlias(field->result_name, unique_name));
                        distinct_columns.emplace(unique_name);
                    }
                    else
                    {
                        required_columns.emplace_back(NameWithAlias(field->result_name, field->result_name));
                        distinct_columns.emplace(field->result_name);
                    }
                }
                else if (expr.has_scalar_function())
                {
                    std::string name;
                    std::vector<String> useless;
                    actions_dag = parseFunction(query_plan->getCurrentDataStream(), expr, name, useless, actions_dag, true);
                    if (!name.empty())
                    {
                        if (distinct_columns.contains(name))
                        {
                            auto unique_name = getUniqueName(name);
                            required_columns.emplace_back(NameWithAlias(name, unique_name));
                            distinct_columns.emplace(unique_name);
                        }
                        else
                        {
                            required_columns.emplace_back(NameWithAlias(name, name));
                            distinct_columns.emplace(name);
                        }
                    }
                }
                else if (expr.has_cast() || expr.has_if_then() || expr.has_literal())
                {
                    const auto * node = parseArgument(actions_dag, expr);
                    actions_dag->addOrReplaceInIndex(*node);
                    if (distinct_columns.contains(node->result_name))
                    {
                        auto unique_name = getUniqueName(node->result_name);
                        required_columns.emplace_back(NameWithAlias(node->result_name, unique_name));
                        distinct_columns.emplace(unique_name);
                    }
                    else
                    {
                        required_columns.emplace_back(NameWithAlias(node->result_name, node->result_name));
                        distinct_columns.emplace(node->result_name);
                    }
                }
                else
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "unsupported projection type {}.", magic_enum::enum_name(expr.rex_type_case()));
                }
            }
            actions_dag->project(required_columns);
            auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
            expression_step->setStepDescription("Project");
            query_plan->addStep(std::move(expression_step));
            break;
        }
        case substrait::Rel::RelTypeCase::kAggregate: {
            const auto & aggregate = rel.aggregate();
            query_plan = parseOp(aggregate.input());
            bool is_final;
            auto aggregate_step = parseAggregate(*query_plan, aggregate, is_final);

            query_plan->addStep(std::move(aggregate_step));

            if (is_final)
            {
                std::vector<int32_t> measure_positions;
                std::vector<substrait::Type> measure_types;
                for (int i = 0; i < aggregate.measures_size(); i++)
                {
                    auto position
                        = aggregate.measures(i).measure().arguments(0).value().selection().direct_reference().struct_field().field();
                    measure_positions.emplace_back(position);
                    measure_types.emplace_back(aggregate.measures(i).measure().output_type());
                }
                auto source = query_plan->getCurrentDataStream().header.getColumnsWithTypeAndName();
                auto target = source;

                for (size_t i = 0; i < measure_positions.size(); i++)
                {
                    if (!isTypeSame(measure_types[i], source[measure_positions[i]].type))
                    {
                        auto target_type = parseType(measure_types[i]);
                        target[measure_positions[i]].type = target_type;
                        target[measure_positions[i]].column = target_type->createColumn();
                    }
                }
                ActionsDAGPtr convert_action
                    = ActionsDAG::makeConvertingActions(source, target, DB::ActionsDAG::MatchColumnsMode::Position);
                if (convert_action)
                {
                    QueryPlanStepPtr convert_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), convert_action);
                    convert_step->setStepDescription("Convert Aggregate Output");
                    query_plan->addStep(std::move(convert_step));
                }
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kRead: {
            const auto & read = rel.read();
            assert(read.has_local_files() || read.has_extension_table() && "Only support local parquet files or merge tree read rel");
            if (read.has_local_files())
            {
                if (isReadRelFromJava(read))
                {
                    query_plan = parseReadRealWithJavaIter(read);
                }
                else
                {
                    query_plan = parseReadRealWithLocalFile(read);
                }
            }
            else
            {
                query_plan = parseMergeTreeTable(read);
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kJoin: {
            const auto & join = rel.join();
            if (!join.has_left() || !join.has_right())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");
            }
            auto left_plan = parseOp(join.left());
            auto right_plan = parseOp(join.right());

            query_plan = parseJoin(join, std::move(left_plan), std::move(right_plan));
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support relation type");
    }
    return query_plan;
}

AggregateFunctionPtr getAggregateFunction(const std::string & name, DataTypes arg_types)
{
    auto & factory = AggregateFunctionFactory::instance();
    AggregateFunctionProperties properties;
    return factory.get(name, arg_types, Array{}, properties);
}

QueryPlanStepPtr SerializedPlanParser::parseAggregate(QueryPlan & plan, const substrait::AggregateRel & rel, bool & is_final)
{
    std::set<substrait::AggregationPhase> phase_set;
    for (int i = 0; i < rel.measures_size(); ++i)
    {
        const auto & measure = rel.measures(i);
        phase_set.emplace(measure.measure().phase());
    }
    if (phase_set.size() > 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "too many aggregate phase!");
    }
    bool final = true;
    if (phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
        || phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE))
        final = false;
    bool only_merge = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);

    is_final = final;

    auto input = plan.getCurrentDataStream();
    ActionsDAGPtr expression = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input.header));
    std::vector<std::string> measure_names;
    std::vector<String> required_columns;
    std::vector<std::string> nullable_measure_names;
    String measure_name;
    for (const auto & measure : rel.measures())
    {
        auto which_measure_type = WhichDataType(parseType(measure.measure().output_type()));
        if (measure.measure().arguments_size() != 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "only support one argument aggregate function");
        }
        auto arg = measure.measure().arguments(0).value();

        if (arg.has_scalar_function())
        {
            parseFunction(input, arg, measure_name, required_columns, expression, true);
            measure_names.emplace_back(measure_name);
        }
        else if (arg.has_selection())
        {
            measure_name = input.header.getByPosition(arg.selection().direct_reference().struct_field().field()).name;
            measure_names.emplace_back(measure_name);
        }
        else if (arg.has_literal())
        {
            const auto * node = parseArgument(expression, arg);
            expression->addOrReplaceInIndex(*node);
            measure_name = node->result_name;
            measure_names.emplace_back(measure_name);
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported aggregate argument type {}.", arg.DebugString());
        }
        if (which_measure_type.isNullable())
        {
            nullable_measure_names.emplace_back(measure_name);
        }
    }
    if (!final)
    {
        wrapNullable(nullable_measure_names, expression);
    }
    auto expression_before_aggregate = std::make_unique<ExpressionStep>(input, expression);
    expression_before_aggregate->setStepDescription("Before Aggregate");
    plan.addStep(std::move(expression_before_aggregate));

    ColumnNumbers keys = {};
    if (rel.groupings_size() == 1)
    {
        for (const auto & group : rel.groupings(0).grouping_expressions())
        {
            if (group.has_selection() && group.selection().has_direct_reference())
            {
                keys.emplace_back(group.selection().direct_reference().struct_field().field());
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported group expression: {}", group.DebugString());
            }
        }
    }
    // only support one grouping or no grouping
    else if (rel.groupings_size() != 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "too many groupings");
    }

    auto aggregates = AggregateDescriptions();
    for (int i = 0; i < rel.measures_size(); ++i)
    {
        const auto & measure = rel.measures(i);
        AggregateDescription agg;
        auto function_signature = this->function_mapping.at(std::to_string(measure.measure().function_reference()));
        auto function_name_idx = function_signature.find(':');
        //        assert(function_name_idx != function_signature.npos && ("invalid function signature: " + function_signature).c_str());
        auto function_name = function_signature.substr(0, function_name_idx);
        if (only_merge)
        {
            agg.column_name = measure_names.at(i);
        }
        else
        {
            agg.column_name = function_name + "(" + measure_names.at(i) + ")";
        }
        agg.arguments = ColumnNumbers{plan.getCurrentDataStream().header.getPositionByName(measure_names.at(i))};
        auto arg_type = plan.getCurrentDataStream().header.getByName(measure_names.at(i)).type;
        if (const auto * function_type = checkAndGetDataType<DataTypeAggregateFunction>(arg_type.get()))
        {
            agg.function = getAggregateFunction(function_name + "Merge", {arg_type});
        }
        else
        {
            auto arg = arg_type;
            if (only_merge)
            {
                auto first = getAggregateFunction(function_name, {arg_type});
                arg = first->getStateType();
                function_name = function_name + "Merge";
            }

            agg.function = getAggregateFunction(function_name, {arg});
        }
        aggregates.push_back(agg);
    }

    if (only_merge)
    {
        auto transform_params = std::make_shared<AggregatingTransformParams>(
            this->getMergedAggregateParam(plan.getCurrentDataStream().header, keys, aggregates), final);
        return std::make_unique<MergingAggregatedStep>(plan.getCurrentDataStream(), transform_params, false, 1, 1);
    }
    else
    {
        auto aggregating_step = std::make_unique<AggregatingStep>(
            plan.getCurrentDataStream(),
            this->getAggregateParam(plan.getCurrentDataStream().header, keys, aggregates),
            final,
            1000000,
            1,
            1,
            1,
            false,
            nullptr,
            SortDescription());
        return std::move(aggregating_step);
    }
}

NamesAndTypesList SerializedPlanParser::blockToNameAndTypeList(const Block & header)
{
    NamesAndTypesList types;
    for (const auto & name : header.getNames())
    {
        const auto * column = header.findByName(name);
        types.push_back(NameAndTypePair(column->name, column->type));
    }
    return types;
}

std::string SerializedPlanParser::getFunctionName(const std::string & function_signature, const substrait::Expression_ScalarFunction & function)
{
    const auto & output_type = function.output_type();
    auto args = function.arguments();
    auto function_name_idx = function_signature.find(':');
    auto function_name = function_signature.substr(0, function_name_idx);
    if (!SCALAR_FUNCTIONS.contains(function_name))
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unsupported function {}", function_name);

    std::string ch_function_name;
    if (function_name == "cast")
    {
        ch_function_name = getCastFunction(output_type);
    }
    else if (function_name == "extract")
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "extract function requires two args.");

        // Get the first arg: field
        const auto & extract_field = args.at(0);

        if (extract_field.value().has_literal())
        {
            const auto & field_value = extract_field.value().literal().string();
            if (field_value == "YEAR")
            {
                ch_function_name = "toYear";
            }
            else if (field_value == "MONTH")
            {
                ch_function_name = "toMonth";
            }
            else if (field_value == "DAYOFWEEK")
            {
                ch_function_name = "toDayOfWeek";
            }
            else if (field_value == "DAYOFYEAR")
            {
                ch_function_name = "toDayOfYear";
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first arg of extract function is wrong.");
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first arg of extract function is wrong.");
    }
    else
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);

    return ch_function_name;
}

const ActionsDAG::Node * SerializedPlanParser::parseFunctionWithDAG(
    const substrait::Expression & rel,
    std::string & result_name,
    std::vector<String> & required_columns,
    DB::ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!rel.has_scalar_function())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "the root of expression should be a scalar function");
    }
    const auto & scalar_function = rel.scalar_function();

    auto function_signature = this->function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = getFunctionName(function_signature, scalar_function);
    ActionsDAG::NodeRawConstPtrs args;
    for (const auto & arg : scalar_function.arguments())
    {
        if (arg.value().has_scalar_function())
        {
            std::string arg_name;
            bool keep_arg = FUNCTION_NEED_KEEP_ARGUMENTS.contains(function_name);
            parseFunctionWithDAG(arg.value(), arg_name, required_columns, actions_dag, keep_arg);
            args.emplace_back(&actions_dag->getNodes().back());
        }
        else
        {
            args.emplace_back(parseArgument(actions_dag, arg.value()));
        }
    }

    const ActionsDAG::Node * result_node;
    if (function_name == "alias")
    {
        result_name = args[0]->result_name;
        actions_dag->addOrReplaceInIndex(*args[0]);
        result_node = &actions_dag->addAlias(actions_dag->findInIndex(result_name), result_name);
    }
    else
    {
        if (function_name == "isNotNull")
        {
            required_columns.emplace_back(args[0]->result_name);
        }
        if (function_signature.find("extract:", 0) != function_signature.npos)
        {
            // delete the first arg
            args.erase(args.begin());
        }
        auto function_builder = FunctionFactory::instance().get(function_name, this->context);
        std::string args_name;
        join(args, ',', args_name);
        result_name = function_name + "(" + args_name + ")";
        const auto * function_node = &actions_dag->addFunction(function_builder, args, result_name);
        result_node = function_node;
        if (!isTypeSame(rel.scalar_function().output_type(), function_node->result_type))
        {
            auto cast_function = getCastFunction(rel.scalar_function().output_type());
            DB::ActionsDAG::NodeRawConstPtrs cast_args({function_node});
            auto cast = FunctionFactory::instance().get(cast_function, this->context);
            std::string cast_args_name;
            join(cast_args, ',', cast_args_name);
            result_name = cast_function + "(" + cast_args_name + ")";
            const auto * cast_node = &actions_dag->addFunction(cast, cast_args, result_name);
            result_node = cast_node;
        }
        if (keep_result)
            actions_dag->addOrReplaceInIndex(*result_node);
    }
    return result_node;
}

ActionsDAGPtr SerializedPlanParser::parseFunction(
    const DataStream & input,
    const substrait::Expression & rel,
    std::string & result_name,
    std::vector<String> & required_columns,
    ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!actions_dag)
    {
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input.header));
    }
    parseFunctionWithDAG(rel, result_name, required_columns, actions_dag, keep_result);
    return actions_dag;
}

const ActionsDAG::Node *
SerializedPlanParser::toFunctionNode(ActionsDAGPtr action_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args)
{
    auto function_builder = DB::FunctionFactory::instance().get(function, this->context);
    std::string args_name;
    join(args, ',', args_name);
    auto result_name = function + "(" + args_name + ")";
    const auto * function_node = &action_dag->addFunction(function_builder, args, result_name);
    return function_node;
}

const ActionsDAG::Node * SerializedPlanParser::parseArgument(ActionsDAGPtr action_dag, const substrait::Expression & rel)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            const auto & literal = rel.literal();
            switch (literal.literal_type_case())
            {
                case substrait::Expression_Literal::kFp64: {
                    auto type = std::make_shared<DataTypeFloat64>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.fp64()), type, getUniqueName(std::to_string(literal.fp64()))));
                }
                case substrait::Expression_Literal::kFp32: {
                    auto type = std::make_shared<DataTypeFloat32>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.fp32()), type, getUniqueName(std::to_string(literal.fp32()))));
                }
                case substrait::Expression_Literal::kString: {
                    auto type = std::make_shared<DataTypeString>();
                    return &action_dag->addColumn(
                        ColumnWithTypeAndName(type->createColumnConst(1, literal.string()), type, getUniqueName(literal.string())));
                }
                case substrait::Expression_Literal::kI64: {
                    auto type = std::make_shared<DataTypeInt64>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.i64()), type, getUniqueName(std::to_string(literal.i64()))));
                }
                case substrait::Expression_Literal::kI32: {
                    auto type = std::make_shared<DataTypeInt32>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.i32()), type, getUniqueName(std::to_string(literal.i32()))));
                }
                case substrait::Expression_Literal::kBoolean: {
                    auto type = std::make_shared<DataTypeUInt8>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.boolean() ? 1 : 0), type, getUniqueName(std::to_string(literal.boolean()))));
                }
                case substrait::Expression_Literal::kI16: {
                    auto type = std::make_shared<DataTypeInt16>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.i16()), type, getUniqueName(std::to_string(literal.i16()))));
                }
                case substrait::Expression_Literal::kI8: {
                    auto type = std::make_shared<DataTypeInt8>();
                    return &action_dag->addColumn(
                        ColumnWithTypeAndName(type->createColumnConst(1, literal.i8()), type, getUniqueName(std::to_string(literal.i8()))));
                }
                case substrait::Expression_Literal::kDate: {
                    auto type = std::make_shared<DataTypeDate32>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.date()), type, getUniqueName(std::to_string(literal.date()))));
                }
                case substrait::Expression_Literal::kTimestamp: {
                    auto type = std::make_shared<DataTypeDateTime64>(6);
                    auto field = DecimalField<DateTime64>(literal.timestamp(), 6);
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, field), type, getUniqueName(std::to_string(literal.timestamp()))));
                }
                case substrait::Expression_Literal::kList: {
                    SizeLimits limit;
                    if (literal.has_empty_list())
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "empty list not support!");
                    }
                    MutableColumnPtr values;
                    DataTypePtr type;
                    auto first_value = literal.list().values(0);
                    if (first_value.has_boolean())
                    {
                        type = std::make_shared<DataTypeUInt8>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).boolean() ? 1 : 0);
                        }
                    }
                    else if (first_value.has_i8())
                    {
                        type = std::make_shared<DataTypeInt8>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).i8());
                        }
                    }
                    else if (first_value.has_i16())
                    {
                        type = std::make_shared<DataTypeInt16>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).i16());
                        }
                    }
                    else if (first_value.has_i32())
                    {
                        type = std::make_shared<DataTypeInt32>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).i32());
                        }
                    }
                    else if (first_value.has_i64())
                    {
                        type = std::make_shared<DataTypeInt64>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).i64());
                        }
                    }
                    else if (first_value.has_fp32())
                    {
                        type = std::make_shared<DataTypeFloat32>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).fp32());
                        }
                    }
                    else if (first_value.has_fp64())
                    {
                        type = std::make_shared<DataTypeFloat64>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).fp64());
                        }
                    }
                    else if (first_value.has_date())
                    {
                        type = std::make_shared<DataTypeDate32>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).date());
                        }
                    }
                    else if (first_value.has_timestamp())
                    {
                        type = std::make_shared<DataTypeDateTime64>(6);
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            auto field = DecimalField<DateTime64>(literal.list().values(i).timestamp(), 6);
                            values->insert(literal.list().values(i).timestamp());
                        }
                    }
                    else if (first_value.has_string())
                    {
                        type = std::make_shared<DataTypeString>();
                        values = type->createColumn();
                        for (int i = 0; i < literal.list().values_size(); ++i)
                        {
                            values->insert(literal.list().values(i).string());
                        }
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::UNKNOWN_TYPE,
                            "unsupported literal list type. {}",
                            magic_enum::enum_name(first_value.literal_type_case()));
                    }
                    auto set = std::make_shared<Set>(limit, true, false);
                    Block values_block;
                    auto name = getUniqueName("__set");
                    values_block.insert(ColumnWithTypeAndName(std::move(values), type, name));
                    set->setHeader(values_block.getColumnsWithTypeAndName());
                    set->insertFromBlock(values_block.getColumnsWithTypeAndName());
                    set->finishInsert();

                    auto arg = ColumnSet::create(set->getTotalRowCount(), set);
                    return &action_dag->addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), name));
                }
                case substrait::Expression_Literal::kNull: {
                    DataTypePtr nested_type;
                    if (literal.null().has_i8())
                    {
                        nested_type = std::make_shared<DataTypeInt8>();
                    }
                    else if (literal.null().has_i16())
                    {
                        nested_type = std::make_shared<DataTypeInt16>();
                    }
                    else if (literal.null().has_i32())
                    {
                        nested_type = std::make_shared<DataTypeInt32>();
                    }
                    else if (literal.null().has_i64())
                    {
                        nested_type = std::make_shared<DataTypeInt64>();
                    }
                    else if (literal.null().has_bool_())
                    {
                        nested_type = std::make_shared<DataTypeUInt8>();
                    }
                    else if (literal.null().has_fp32())
                    {
                        nested_type = std::make_shared<DataTypeFloat32>();
                    }
                    else if (literal.null().has_fp64())
                    {
                        nested_type = std::make_shared<DataTypeFloat64>();
                    }
                    else if (literal.null().has_date())
                    {
                        nested_type = std::make_shared<DataTypeDate32>();
                    }
                    else if (literal.null().has_timestamp())
                    {
                        nested_type = std::make_shared<DataTypeDateTime64>(6);
                    }
                    else if (literal.null().has_string())
                    {
                        nested_type = std::make_shared<DataTypeString>();
                    }
                    auto type = std::make_shared<DataTypeNullable>(nested_type);
                    return &action_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, Field()), type, getUniqueName("null")));
                }
                default: {
                    throw Exception(
                        ErrorCodes::UNKNOWN_TYPE, "unsupported constant type {}", magic_enum::enum_name(literal.literal_type_case()));
                }
            }
        }
        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");
            }
            const auto * field = action_dag->getInputs()[rel.selection().direct_reference().struct_field().field()];
            return action_dag->tryFindInIndex(field->result_name);
        }
        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");

            std::string ch_function_name = getCastFunction(rel.cast().type());
            DB::ActionsDAG::NodeRawConstPtrs args;
            auto cast_input = rel.cast().input();
            if (cast_input.has_selection())
            {
                args.emplace_back(parseArgument(action_dag, rel.cast().input()));
            }
            else if (cast_input.has_if_then())
            {
                args.emplace_back(parseArgument(action_dag, rel.cast().input()));
            }
            else if (cast_input.has_scalar_function())
            {
                std::string result;
                std::vector<String> useless;
                const auto * node = parseFunctionWithDAG(cast_input, result, useless, action_dag, false);
                args.emplace_back(node);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported cast input {}", rel.cast().input().DebugString());
            }
            const auto * function_node = toFunctionNode(action_dag, ch_function_name, args);
            action_dag->addOrReplaceInIndex(*function_node);
            return function_node;
        }
        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            auto function_multi_if = DB::FunctionFactory::instance().get("multiIf", this->context);
            DB::ActionsDAG::NodeRawConstPtrs args;

            auto condition_nums = if_then.ifs_size();
            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                std::string if_name;
                std::string then_name;
                std::vector<String> useless;
                parseFunctionWithDAG(ifs.if_(), if_name, useless, action_dag, false);
                for (const auto & node : action_dag->getNodes())
                {
                    if (node.result_name == if_name)
                    {
                        args.emplace_back(&node);
                        break;
                    }
                }
                const auto * then_node = parseArgument(action_dag, ifs.then());
                args.emplace_back(then_node);
            }

            const auto * else_node = parseArgument(action_dag, if_then.else_());
            args.emplace_back(else_node);
            std::string args_name;
            join(args, ',', args_name);
            auto result_name = "multiIf(" + args_name + ")";
            const auto * function_node = &action_dag->addFunction(function_multi_if, args, result_name);
            action_dag->addOrReplaceInIndex(*function_node);
            return function_node;
        }
        case substrait::Expression::RexTypeCase::kScalarFunction: {
            std::string result;
            std::vector<String> useless;
            return parseFunctionWithDAG(rel, result, useless, action_dag, false);
        }
        case substrait::Expression::RexTypeCase::kSingularOrList: {
            DB::ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(parseArgument(action_dag, rel.singular_or_list().value()));
            const auto & options = rel.singular_or_list().options();

            SizeLimits limit;
            if (options.empty())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "empty list not support!");
            }
            ColumnPtr values;
            DataTypePtr type;
            if (!options[0].has_literal())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
            }
            auto first_value = options[0].literal();
            using FieldGetter = std::function<Field (substrait::Expression_Literal)>;
            auto fill_values = [options](DataTypePtr type, FieldGetter getter) -> ColumnPtr {
                auto values = type->createColumn();
                for (const auto & v : options)
                {
                    values->insert(getter(v.literal()));
                }
                return values;
            };
            if (first_value.has_boolean())
            {
                type = std::make_shared<DataTypeUInt8>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.boolean() ? 1 : 0;});
            }
            else if (first_value.has_i8())
            {
                type = std::make_shared<DataTypeInt8>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.i8();});
            }
            else if (first_value.has_i16())
            {
                type = std::make_shared<DataTypeInt16>();
                values = type->createColumn();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.i16();});
            }
            else if (first_value.has_i32())
            {
                type = std::make_shared<DataTypeInt32>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.i32();});
            }
            else if (first_value.has_i64())
            {
                type = std::make_shared<DataTypeInt64>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.i64();});
            }
            else if (first_value.has_fp32())
            {
                type = std::make_shared<DataTypeFloat32>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.fp32();});
            }
            else if (first_value.has_fp64())
            {
                type = std::make_shared<DataTypeFloat64>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.fp64();});
            }
            else if (first_value.has_date())
            {
                type = std::make_shared<DataTypeDate32>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.date();});
            }
            else if (first_value.has_string())
            {
                type = std::make_shared<DataTypeString>();
                values = fill_values(type, [](substrait::Expression_Literal expr) -> Field {return expr.string();});
            }
            else
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_TYPE,
                    "unsupported literal list type. {}",
                    magic_enum::enum_name(first_value.literal_type_case()));
            }
            auto set = std::make_shared<Set>(limit, true, false);
            Block values_block;
            auto name = getUniqueName("__set");
            values_block.insert(ColumnWithTypeAndName(values, type, name));
            set->setHeader(values_block.getColumnsWithTypeAndName());
            set->insertFromBlock(values_block.getColumnsWithTypeAndName());
            set->finishInsert();

            auto arg = ColumnSet::create(set->getTotalRowCount(), set);
            args.emplace_back(&action_dag->addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), name)));

            const auto * function_node = toFunctionNode(action_dag, "in", args);
            action_dag->addOrReplaceInIndex(*function_node);
            return function_node;
        }
        default: {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "unsupported arg type {} : {}", magic_enum::enum_name(rel.rex_type_case()), rel.DebugString());
        }
    }
}

QueryPlanPtr SerializedPlanParser::parse(const std::string & plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    plan_ptr->ParseFromString(plan);
    LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "parse plan \n{}", plan_ptr->DebugString());
    return parse(std::move(plan_ptr));
}
void SerializedPlanParser::initFunctionEnv()
{
    registerFunctions();
    registerAggregateFunctions();
}
SerializedPlanParser::SerializedPlanParser(const ContextPtr & context_) : context(context_)
{
}
ContextMutablePtr SerializedPlanParser::global_context = nullptr;

Context::ConfigurationPtr SerializedPlanParser::config = nullptr;

void SerializedPlanParser::collectJoinKeys(
    const substrait::Expression & condition, std::vector<std::pair<int32_t, int32_t>> & join_keys, int32_t right_key_start)
{
    auto condition_name = getFunctionName(
        this->function_mapping.at(std::to_string(condition.scalar_function().function_reference())), condition.scalar_function());
    if (condition_name == "and")
    {
        collectJoinKeys(condition.scalar_function().arguments(0).value(), join_keys, right_key_start);
        collectJoinKeys(condition.scalar_function().arguments(1).value(), join_keys, right_key_start);
    }
    else if (condition_name == "equals")
    {
        const auto & function = condition.scalar_function();
        auto left_key_idx = function.arguments(0).value().selection().direct_reference().struct_field().field();
        auto right_key_idx = function.arguments(1).value().selection().direct_reference().struct_field().field() - right_key_start;
        join_keys.emplace_back(std::pair(left_key_idx, right_key_idx));
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "doesn't support condition {}", condition_name);
    }
}

DB::QueryPlanPtr SerializedPlanParser::parseJoin(substrait::JoinRel join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    auto join_opt_info = parseJoinOptimizationInfo(join.advanced_extension().optimization().value());
    auto table_join = std::make_shared<TableJoin>(global_context->getSettings(), global_context->getTemporaryVolume());
    if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_INNER)
    {
        table_join->setKind(DB::ASTTableJoin::Kind::Inner);
        table_join->setStrictness(DB::ASTTableJoin::Strictness::All);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_SEMI)
    {
        table_join->setKind(DB::ASTTableJoin::Kind::Left);
        table_join->setStrictness(DB::ASTTableJoin::Strictness::Semi);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_ANTI)
    {
        table_join->setKind(DB::ASTTableJoin::Kind::Left);
        table_join->setStrictness(DB::ASTTableJoin::Strictness::Anti);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_LEFT)
    {
        table_join->setKind(DB::ASTTableJoin::Kind::Left);
        table_join->setStrictness(DB::ASTTableJoin::Strictness::All);
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join.type()));
    }

    if (join_opt_info.is_broadcast)
    {
        auto storage_join = BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key);
        ActionsDAGPtr project = ActionsDAG::makeConvertingActions(
            right->getCurrentDataStream().header.getColumnsWithTypeAndName(),
            storage_join->getRightSampleBlock().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        if (project)
        {
            QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), project);
            project_step->setStepDescription("Rename Broadcast Table Name");
            right->addStep(std::move(project_step));
        }
    }

    table_join->addDisjunct();
    table_join->setColumnsFromJoinedTable(right->getCurrentDataStream().header.getNamesAndTypesList());

    NameSet left_columns_set;
    for (const auto & col : left->getCurrentDataStream().header.getNames())
    {
        left_columns_set.emplace(col);
    }
    table_join->deduplicateAndQualifyColumnNames(left_columns_set, getUniqueName("right") + ".");
    // fix right table key duplicate
    NamesWithAliases right_table_alias;
    for (size_t idx = 0; idx < table_join->columnsFromJoinedTable().size(); idx++)
    {
        auto origin_name = right->getCurrentDataStream().header.getByPosition(idx).name;
        auto dedup_name = table_join->columnsFromJoinedTable().getNames().at(idx);
        if (origin_name != dedup_name)
        {
            right_table_alias.emplace_back(NameWithAlias(origin_name, dedup_name));
        }
    }
    if (!right_table_alias.empty())
    {
        ActionsDAGPtr project = std::make_shared<ActionsDAG>(right->getCurrentDataStream().header.getNamesAndTypesList());
        project->addAliases(right_table_alias);
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), project);
        project_step->setStepDescription("Right Table Rename");
        right->addStep(std::move(project_step));
    }
    // support multiple join key
    std::vector<std::pair<int32_t, int32_t>> join_keys;
    collectJoinKeys(join.expression(), join_keys, left->getCurrentDataStream().header.columns());
    for (auto key : join_keys)
    {
        ASTPtr left_key = std::make_shared<ASTIdentifier>(left->getCurrentDataStream().header.getByPosition(key.first).name);
        ASTPtr right_key = std::make_shared<ASTIdentifier>(right->getCurrentDataStream().header.getByPosition(key.second).name);
        table_join->addOnKeys(left_key, right_key);
    }

    for (const auto & column : table_join->columnsFromJoinedTable())
    {
        table_join->addJoinedColumn(column);
    }
    ActionsDAGPtr left_convert_actions = nullptr;
    ActionsDAGPtr right_convert_actions = nullptr;
    std::tie(left_convert_actions, right_convert_actions) = table_join->createConvertingActions(
        left->getCurrentDataStream().header.getColumnsWithTypeAndName(), right->getCurrentDataStream().header.getColumnsWithTypeAndName());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        right->addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(left->getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        left->addStep(std::move(converting_step));
    }
    QueryPlanPtr query_plan;
    Names after_join_names;
    auto left_names = left->getCurrentDataStream().header.getNames();
    after_join_names.insert(after_join_names.end(), left_names.begin(), left_names.end());
    auto right_name = table_join->columnsFromJoinedTable().getNames();
    after_join_names.insert(after_join_names.end(), right_name.begin(), right_name.end());

    if (join_opt_info.is_broadcast)
    {
        auto storage_join = BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key);
        if (!storage_join)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "broad cast table {} not found.", join_opt_info.storage_join_key);
        }
        auto hash_join = storage_join->getJoinLocked(table_join, context);
        QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentDataStream(), hash_join, 8192);

        join_step->setStepDescription("JOIN");
        left->addStep(std::move(join_step));
        query_plan = std::move(left);
    }
    else
    {
        auto hash_join = std::make_shared<HashJoin>(table_join, right->getCurrentDataStream().header.cloneEmpty());
        QueryPlanStepPtr join_step
            = std::make_unique<DB::JoinStep>(left->getCurrentDataStream(), right->getCurrentDataStream(), hash_join, 8192);

        join_step->setStepDescription("JOIN");

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    }

    reorderJoinOutput(*query_plan, after_join_names);
    if (join.has_post_join_filter())
    {
        std::string filter_name;
        std::vector<String> useless;
        auto actions_dag = parseFunction(query_plan->getCurrentDataStream(), join.post_join_filter(), filter_name, useless, nullptr, true);
        auto filter_step = std::make_unique<FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
        filter_step->setStepDescription("Post Join Filter");
        query_plan->addStep(std::move(filter_step));
    }
    return query_plan;
}
void SerializedPlanParser::reorderJoinOutput(QueryPlan & plan, DB::Names cols)
{
    ActionsDAGPtr project = std::make_shared<ActionsDAG>(plan.getCurrentDataStream().header.getNamesAndTypesList());
    NamesWithAliases project_cols;
    for (const auto & col : cols)
    {
        project_cols.emplace_back(NameWithAlias(col, col));
    }
    project->project(project_cols);
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), project);
    project_step->setStepDescription("Reorder Join Output");
    plan.addStep(std::move(project_step));
}
void SerializedPlanParser::removeNullable(std::vector<String> require_columns, ActionsDAGPtr actionsDag)
{
    for (const auto & item : require_columns)
    {
        auto function_builder = FunctionFactory::instance().get("assumeNotNull", this->context);
        ActionsDAG::NodeRawConstPtrs args;
        args.emplace_back(&actionsDag->findInIndex(item));
        const auto & node = actionsDag->addFunction(function_builder, args, item);
        actionsDag->addOrReplaceInIndex(node);
    }
}

void SerializedPlanParser::wrapNullable(std::vector<String> columns, ActionsDAGPtr actionsDag)
{
    for (const auto & item : columns)
    {
        auto function_builder = FunctionFactory::instance().get("toNullable", this->context);
        ActionsDAG::NodeRawConstPtrs args;
        args.emplace_back(&actionsDag->findInIndex(item));
        const auto & node = actionsDag->addFunction(function_builder, args, item);
        actionsDag->addOrReplaceInIndex(node);
    }
}

SharedContextHolder SerializedPlanParser::shared_context;

void LocalExecutor::execute(QueryPlanPtr query_plan)
{
    Stopwatch stopwatch;
    stopwatch.start();
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = true};
    auto pipeline_builder = query_plan->buildQueryPipeline(
        optimization_settings,
        BuildQueryPipelineSettings{
            .actions_settings = ExpressionActionsSettings{
                .can_compile_expressions = true, .min_count_to_compile_expression = 3, .compile_expressions = CompileExpressions::yes}});
    this->query_pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    auto t_pipeline = stopwatch.elapsedMicroseconds();
    this->executor = std::make_unique<PullingPipelineExecutor>(query_pipeline);
    auto t_executor = stopwatch.elapsedMicroseconds() - t_pipeline;
    stopwatch.stop();
    LOG_INFO(
        &Poco::Logger::get("SerializedPlanParser"),
        "build pipeline {} ms; create executor {} ms;",
        t_pipeline / 1000.0,
        t_executor / 1000.0);
    this->header = query_plan->getCurrentDataStream().header.cloneEmpty();
    this->ch_column_to_spark_row = std::make_unique<CHColumnToSparkRow>();
}
std::unique_ptr<SparkRowInfo> LocalExecutor::writeBlockToSparkRow(Block & block)
{
    return this->ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}
bool LocalExecutor::hasNext()
{
    bool has_next;
    if (currentBlock().columns() == 0 || isConsumed())
    {
        auto empty_block = header.cloneEmpty();
        setCurrentBlock(empty_block);
        has_next = this->executor->pull(currentBlock());
        produce();
    }
    else
    {
        has_next = true;
    }
    return has_next;
}
SparkRowInfoPtr LocalExecutor::next()
{
    checkNextValid();
    SparkRowInfoPtr row_info = writeBlockToSparkRow(currentBlock());
    produce();
    if (this->spark_buffer)
    {
        this->ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        this->spark_buffer.reset();
    }
    this->spark_buffer = std::make_unique<SparkBuffer>();
    this->spark_buffer->address = row_info->getBufferAddress();
    this->spark_buffer->size = row_info->getTotalBytes();
    return row_info;
}

Block * LocalExecutor::nextColumnar()
{
    checkNextValid();
    Block * columnar_batch;
    if (currentBlock().columns() > 0)
    {
        columnar_batch = &currentBlock();
    }
    else
    {
        auto empty_block = header.cloneEmpty();
        setCurrentBlock(empty_block);
        columnar_batch = &currentBlock();
    }
    consume();
    return columnar_batch;
}

Block & LocalExecutor::getHeader()
{
    return header;
}
LocalExecutor::LocalExecutor(QueryContext & _query_context) : query_context(_query_context)
{
}
}
