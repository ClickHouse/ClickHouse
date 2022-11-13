#include <memory>
#include <base/logger_useful.h>
#include <base/Decimal.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Builder/BroadCastJoinBuilder.h>
#include <Columns/ColumnSet.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
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
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <base/logger_useful.h>
#include <google/protobuf/util/json_util.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/DebugUtils.h>
#include <Common/JoinHelper.h>
#include <Common/MergeTreeTool.h>
#include <Common/StringUtils.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/IStorage.h>
#include <Common/CHUtil.h>
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
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
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

bool isTypeMatched(const substrait::Type & substrait_type, const DataTypePtr & ch_type)
{
    const auto parsed_ch_type = SerializedPlanParser::parseType(substrait_type);
    return parsed_ch_type->equals(*ch_type);
}

/// TODO: This function needs to be improved for Decimal/Array/Map/Tuple types.
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
    else if (type.has_string() || type.has_binary())
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

    /// TODO(taiyang-li): implement cast functions of other types

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

void SerializedPlanParser::addRemoveNullableStep(QueryPlan & plan, std::vector<String> columns)
{
    if (columns.empty()) return;
    auto remove_nullable_actions_dag
        = std::make_shared<ActionsDAG>(blockToNameAndTypeList(plan.getCurrentDataStream().header));
    removeNullable(columns, remove_nullable_actions_dag);
    auto expression_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), remove_nullable_actions_dag);
    expression_step->setStepDescription("Remove nullable properties");
    plan.addStep(std::move(expression_step));
}

QueryPlanPtr SerializedPlanParser::parseMergeTreeTable(const substrait::ReadRel & rel)
{
    assert(rel.has_extension_table());
    std::string table = rel.extension_table().detail().value();
    auto merge_tree_table = local_engine::parseMergeTreeTableString(table);
    DB::Block header;
    if (rel.has_base_schema() && rel.base_schema().names_size())
    {
        header = parseNameStruct(rel.base_schema());
    }
    else
    {
        // For count(*) case, there will be an empty base_schema, so we try to read at least once column
        auto all_parts_dir = MergeTreeUtil::getAllMergeTreeParts( std::filesystem::path("/") / merge_tree_table.relative_path);
        if (all_parts_dir.empty())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Empty mergetree directory: {}", merge_tree_table.relative_path);
        }
        auto part_names_types_list = MergeTreeUtil::getSchemaFromMergeTreePart(all_parts_dir[0]);
        NamesAndTypesList one_column_name_type;
        one_column_name_type.push_back(part_names_types_list.front());
        header = BlockUtil::buildHeader(one_column_name_type);
        LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "Try to read ({}) instead of empty header", header.dumpNames());
    }
    auto names_and_types_list = header.getNamesAndTypesList();
    auto storage_factory = StorageMergeTreeFactory::instance();
    auto metadata = buildMetaData(names_and_types_list, this->context);
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
    query_context.custom_storage_merge_tree = storage;
    auto query_info = buildQueryInfo(names_and_types_list);
    std::vector<String> not_null_columns;
    if (rel.has_filter())
    {
        query_info->prewhere_info = parsePreWhereInfo(rel.filter(), header, not_null_columns);
    }
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
    if (!not_null_columns.empty())
    {
        auto input_header = query->getCurrentDataStream().header;
        std::erase_if(not_null_columns, [input_header](auto item) -> bool {return !input_header.has(item);});
        addRemoveNullableStep(*query, not_null_columns);
    }
    return query;
}

PrewhereInfoPtr SerializedPlanParser::parsePreWhereInfo(const substrait::Expression & rel, Block & input, std::vector<String>& not_nullable_columns)
{
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = std::make_shared<ActionsDAG>(input.getNamesAndTypesList());
    std::string filter_name;
    // for in function
    if (rel.has_singular_or_list())
    {
        const auto *in_node = parseArgument(prewhere_info->prewhere_actions, rel);
        prewhere_info->prewhere_actions->addOrReplaceInIndex(*in_node);
        filter_name = in_node->result_name;
    }
    else
    {
        parseFunctionWithDAG(rel, filter_name, not_nullable_columns, prewhere_info->prewhere_actions, true);
    }
    prewhere_info->prewhere_column_name = filter_name;
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = true;
    auto cols = prewhere_info->prewhere_actions->getRequiredColumnsNames();
    if (last_project)
    {
        prewhere_info->prewhere_actions->removeUnusedActions(Names{filter_name}, true, true);
        prewhere_info->prewhere_actions->projectInput(false);
        for (const auto & expr : last_project->expressions())
        {
            if (expr.has_selection())
            {
                auto position = expr.selection().direct_reference().struct_field().field();
                auto name = input.getByPosition(position).name;
                prewhere_info->prewhere_actions->tryRestoreColumn(name);
            }
        }
        auto output = prewhere_info->prewhere_actions->getIndex();
    }
    else
    {
        prewhere_info->prewhere_actions->removeUnusedActions(Names{filter_name}, false, true);
        prewhere_info->prewhere_actions->projectInput(false);
        for (const auto& name : input.getNames())
        {
            prewhere_info->prewhere_actions->tryRestoreColumn(name);
        }
    }
    return prewhere_info;
}

Block SerializedPlanParser::parseNameStruct(const substrait::NamedStruct & struct_)
{
    ColumnsWithTypeAndName internal_cols;
    internal_cols.reserve(struct_.names_size());
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
        internal_cols.push_back(ColumnWithTypeAndName(data_type, name));
    }
    Block res(std::move(internal_cols));
    return std::move(res);
}

DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type)
{
    return wrapNullableType(nullable == substrait::Type_Nullability_NULLABILITY_NULLABLE, nested_type);
}

DataTypePtr wrapNullableType(bool nullable, DataTypePtr nested_type)
{
    if (nullable)
        return std::make_shared<DataTypeNullable>(nested_type);
    else
        return nested_type;
}

DataTypePtr SerializedPlanParser::parseType(const substrait::Type & substrait_type)
{
    DataTypePtr ch_type;
    if (substrait_type.has_bool_())
    {
        ch_type = std::make_shared<DataTypeUInt8>();
        ch_type = wrapNullableType(substrait_type.bool_().nullability(), ch_type);
    }
    else if (substrait_type.has_i8())
    {
        ch_type = std::make_shared<DataTypeInt8>();
        ch_type = wrapNullableType(substrait_type.i8().nullability(), ch_type);
    }
    else if (substrait_type.has_i16())
    {
        ch_type = std::make_shared<DataTypeInt16>();
        ch_type = wrapNullableType(substrait_type.i16().nullability(), ch_type);
    }
    else if (substrait_type.has_i32())
    {
        ch_type = std::make_shared<DataTypeInt32>();
        ch_type = wrapNullableType(substrait_type.i32().nullability(), ch_type);
    }
    else if (substrait_type.has_i64())
    {
        ch_type = std::make_shared<DataTypeInt64>();
        ch_type = wrapNullableType(substrait_type.i64().nullability(), ch_type);
    }
    else if (substrait_type.has_string())
    {
        ch_type = std::make_shared<DataTypeString>();
        ch_type = wrapNullableType(substrait_type.string().nullability(), ch_type);
    }
    else if (substrait_type.has_binary())
    {
        ch_type = std::make_shared<DataTypeString>();
        ch_type = wrapNullableType(substrait_type.binary().nullability(), ch_type);
    }
    else if (substrait_type.has_fp32())
    {
        ch_type = std::make_shared<DataTypeFloat32>();
        ch_type = wrapNullableType(substrait_type.fp32().nullability(), ch_type);
    }
    else if (substrait_type.has_fp64())
    {
        ch_type = std::make_shared<DataTypeFloat64>();
        ch_type = wrapNullableType(substrait_type.fp64().nullability(), ch_type);
    }
    else if (substrait_type.has_timestamp())
    {
        ch_type = std::make_shared<DataTypeDateTime64>(6);
        ch_type = wrapNullableType(substrait_type.timestamp().nullability(), ch_type);
    }
    else if (substrait_type.has_date())
    {
        ch_type = std::make_shared<DataTypeDate32>();
        ch_type = wrapNullableType(substrait_type.date().nullability(), ch_type);
    }
    else if (substrait_type.has_decimal())
    {
        UInt32 precision = substrait_type.decimal().precision();
        UInt32 scale = substrait_type.decimal().scale();
        if (precision <= DataTypeDecimal32::maxPrecision())
            ch_type = std::make_shared<DataTypeDecimal32>(precision, scale);
        else if (precision <= DataTypeDecimal64::maxPrecision())
            ch_type = std::make_shared<DataTypeDecimal64>(precision, scale);
        else if (precision <= DataTypeDecimal128::maxPrecision())
            ch_type = std::make_shared<DataTypeDecimal128>(precision, scale);
        else
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);

        ch_type = wrapNullableType(substrait_type.decimal().nullability(), ch_type);
    }
    else if (substrait_type.has_struct_())
    {
        DataTypes ch_field_types(substrait_type.struct_().types().size());
        for (size_t i = 0; i < ch_field_types.size(); ++i)
            ch_field_types[i] = std::move(parseType(substrait_type.struct_().types()[i]));
        ch_type = std::make_shared<DataTypeTuple>(ch_field_types);
        ch_type = wrapNullableType(substrait_type.struct_().nullability(), ch_type);
    }
    else if (substrait_type.has_list())
    {
        auto ch_nested_type = parseType(substrait_type.list().type());
        ch_type = std::make_shared<DataTypeArray>(ch_nested_type);
        ch_type = wrapNullableType(substrait_type.list().nullability(), ch_type);
    }
    else if (substrait_type.has_map())
    {
        auto ch_key_type = parseType(substrait_type.map().key());
        auto ch_val_type = parseType(substrait_type.map().value());
        ch_type = std::make_shared<DataTypeMap>(ch_key_type, ch_val_type);
        ch_type = wrapNullableType(substrait_type.map().nullability(), ch_type);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support type {}", substrait_type.DebugString());

    /// TODO(taiyang-li): consider Time/IntervalYear/IntervalDay/TimestampTZ/UUID/FixedChar/VarChar/FixedBinary/UserDefined
    return std::move(ch_type);
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
        if (root_rel.root().names_size())
        {
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
        }

        if (logger->trace())
        {
            WriteBufferFromOwnString plan_string;
            WriteBufferFromOwnString pipeline_string;
            QueryPlan::ExplainPlanOptions options;
            options.header = true;
            query_plan->explainPlan(plan_string, options);
            LOG_TRACE(
                &Poco::Logger::get("SerializedPlanParser"),
                "<ch plan>: \n{}\n<pipeline output>:\n{}\n",
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
            auto actions_dag = parseFunction(
                query_plan->getCurrentDataStream().header, filter.condition(), filter_name, required_columns, nullptr, true);
            auto input = query_plan->getCurrentDataStream().header.getNames();
            Names input_with_condition(input);
            input_with_condition.emplace_back(filter_name);
            actions_dag->removeUnusedActions(input_with_condition);
            auto filter_step = std::make_unique<FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
            query_plan->addStep(std::move(filter_step));

            // remove nullable
            addRemoveNullableStep(*query_plan, required_columns);
            break;
        }
        case substrait::Rel::RelTypeCase::kProject: {
            const auto & project = rel.project();
            last_project = &project;
            query_plan = parseOp(project.input());
            // for prewhere
            bool is_mergetree_input = project.input().has_read() && !project.input().read().has_local_files();
            Block read_schema;
            if (is_mergetree_input)
            {
                read_schema = parseNameStruct(project.input().read().base_schema());
            }
            else
            {
                read_schema = query_plan->getCurrentDataStream().header;
            }
            const auto & expressions = project.expressions();
            auto actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
            NamesWithAliases required_columns;
            std::set<String> distinct_columns;

            for (const auto & expr : expressions)
            {
                if (expr.has_selection())
                {
                    auto position = expr.selection().direct_reference().struct_field().field();
                    const ActionsDAG::Node * field = actions_dag->tryFindInIndex(read_schema.getByPosition(position).name);
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
                    actions_dag = parseFunction(query_plan->getCurrentDataStream().header, expr, name, useless, actions_dag, true);
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

                bool need_convert = false;
                for (size_t i = 0; i < measure_positions.size(); i++)
                {
                    if (!isTypeMatched(measure_types[i], source[measure_positions[i]].type))
                    {
                        auto target_type = parseType(measure_types[i]);
                        target[measure_positions[i]].type = target_type;
                        target[measure_positions[i]].column = target_type->createColumn();
                        need_convert = true;
                    }
                }

                if (need_convert) {
                    ActionsDAGPtr convert_action
                        = ActionsDAG::makeConvertingActions(source, target, DB::ActionsDAG::MatchColumnsMode::Position);
                    if (convert_action)
                    {
                        QueryPlanStepPtr convert_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), convert_action);
                        convert_step->setStepDescription("Convert Aggregate Output");
                        query_plan->addStep(std::move(convert_step));
                    }
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
            last_project = nullptr;
            break;
        }
        case substrait::Rel::RelTypeCase::kJoin: {
            const auto & join = rel.join();
            if (!join.has_left() || !join.has_right())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");
            }
            last_project = nullptr;
            auto left_plan = parseOp(join.left());
            last_project = nullptr;
            auto right_plan = parseOp(join.right());

            query_plan = parseJoin(join, std::move(left_plan), std::move(right_plan));
            break;
        }
        case substrait::Rel::RelTypeCase::kSort: {
            const auto & sort_rel = rel.sort();
            query_plan = parseSort(sort_rel);
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

void SerializedPlanParser::addPreProjectStepIfNeeded(
    QueryPlan & plan,
    const substrait::AggregateRel & rel,
    std::vector<std::string> & measure_names,
    std::map<std::string, std::string> & nullable_measure_names)
{
    auto input = plan.getCurrentDataStream();
    ActionsDAGPtr expression = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input.header));
    std::vector<String> required_columns;
    std::vector<std::string> to_wrap_nullable;
    String measure_name;
    bool need_pre_project = false;
    for (const auto & measure : rel.measures())
    {
        auto which_measure_type = WhichDataType(parseType(measure.measure().output_type()));
        if (measure.measure().arguments_size() != 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "only support one argument aggregate function");
        }
        auto arg = measure.measure().arguments(0).value();

        if (arg.has_selection())
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
            need_pre_project = true;
        }
        else
        {
            // this includes the arg.has_scalar_function() case
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported aggregate argument type {}.", arg.DebugString());
        }

        if (which_measure_type.isNullable() &&
            measure.measure().phase() == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE &&
            !expression->findInIndex(measure_name).result_type->isNullable()
            )
        {
            to_wrap_nullable.emplace_back(measure_name);
            need_pre_project = true;
        }
    }
    wrapNullable(to_wrap_nullable, expression, nullable_measure_names);

    if (need_pre_project) {
        auto expression_before_aggregate = std::make_unique<ExpressionStep>(input, expression);
        expression_before_aggregate->setStepDescription("Before Aggregate");
        plan.addStep(std::move(expression_before_aggregate));
    }
}


/**
 * Gluten will use a pre projection step (search needsPreProjection in HashAggregateExecBaseTransformer)
 * so this function can assume all group and agg args are direct references or literals
 */
QueryPlanStepPtr SerializedPlanParser::parseAggregate(QueryPlan & plan, const substrait::AggregateRel & rel, bool & is_final)
{
    std::set<substrait::AggregationPhase> phase_set;
    for (int i = 0; i < rel.measures_size(); ++i)
    {
        const auto & measure = rel.measures(i);
        phase_set.emplace(measure.measure().phase());
    }

    bool has_first_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
    bool has_inter_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE);
    bool has_final_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);

    if (phase_set.size() > 1)
    {
        if (phase_set.size() == 2 && has_first_stage && has_inter_stage) {
            // this will happen in a sql like:
            // select sum(a), count(distinct b) from T
        } else {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "too many aggregate phase!");
        }
    }

    is_final = has_final_stage;

    std::vector<std::string> measure_names;
    std::map<std::string, std::string> nullable_measure_names;
    addPreProjectStepIfNeeded(plan, rel, measure_names, nullable_measure_names);

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
        if (measure.measure().phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
        {
            agg.column_name = measure_names.at(i);
        }
        else
        {
            agg.column_name = function_name + "(" + measure_names.at(i) + ")";
        }

        // if measure arg has nullable version, use it
        auto input_column = measure_names.at(i);
        auto entry = nullable_measure_names.find(input_column);
        if (entry != nullable_measure_names.end()) {
            input_column = entry->second;
        }
        agg.arguments = ColumnNumbers{plan.getCurrentDataStream().header.getPositionByName(input_column)};
        auto arg_type = plan.getCurrentDataStream().header.getByName(input_column).type;
        if (const auto * function_type = checkAndGetDataType<DataTypeAggregateFunction>(arg_type.get()))
        {
            auto suffix = "PartialMerge";
            agg.function = getAggregateFunction(function_name + suffix, {arg_type});
        }
        else
        {
            auto arg = arg_type;
            if (measure.measure().phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
            {
                auto first = getAggregateFunction(function_name, {arg_type});
                arg = first->getStateType();
                auto suffix = "PartialMerge";
                function_name = function_name + suffix;
            }

            agg.function = getAggregateFunction(function_name, {arg});
        }
        aggregates.push_back(agg);
    }

    if (has_final_stage)
    {
        auto transform_params = std::make_shared<AggregatingTransformParams>(
            this->getMergedAggregateParam(plan.getCurrentDataStream().header, keys, aggregates), true);
        return std::make_unique<MergingAggregatedStep>(plan.getCurrentDataStream(), transform_params, false, 1, 1);
    }
    else
    {
        auto aggregating_step = std::make_unique<AggregatingStep>(
            plan.getCurrentDataStream(),
            this->getAggregateParam(plan.getCurrentDataStream().header, keys, aggregates),
            false,
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


std::string
SerializedPlanParser::getFunctionName(const std::string & function_signature, const substrait::Expression_ScalarFunction & function)
{
    const auto & output_type = function.output_type();
    auto args = function.arguments();
    auto pos = function_signature.find(':');
    auto function_name = function_signature.substr(0, pos);
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "the root of expression should be a scalar function:\n {}", rel.DebugString());
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
        else if (function_name == "splitByRegexp")
        {
            if (args.size() >= 2)
            {
                /// In Spark: split(str, regex [, limit] )
                /// In CH: splitByRegexp(regexp, s)
                std::swap(args[0], args[1]);
            }
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
        if (!isTypeMatched(rel.scalar_function().output_type(), function_node->result_type))
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
    const Block & input,
    const substrait::Expression & rel,
    std::string & result_name,
    std::vector<String> & required_columns,
    ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!actions_dag)
    {
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input));
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

std::pair<DataTypePtr, Field> SerializedPlanParser::parseLiteral(const substrait::Expression_Literal & literal)
{
    DataTypePtr type;
    Field field;

    switch (literal.literal_type_case())
    {
        case substrait::Expression_Literal::kFp64: {
            type = std::make_shared<DataTypeFloat64>();
            field = literal.fp64();
            break;
        }
        case substrait::Expression_Literal::kFp32: {
            type = std::make_shared<DataTypeFloat32>();
            field = literal.fp32();
            break;
        }
        case substrait::Expression_Literal::kString: {
            type = std::make_shared<DataTypeString>();
            field = literal.string();
            break;
        }
        case substrait::Expression_Literal::kBinary: {
            type = std::make_shared<DataTypeString>();
            field = literal.binary();
            break;
        }
        case substrait::Expression_Literal::kI64: {
            type = std::make_shared<DataTypeInt64>();
            field = literal.i64();
            break;
        }
        case substrait::Expression_Literal::kI32: {
            type = std::make_shared<DataTypeInt32>();
            field = literal.i32();
            break;
        }
        case substrait::Expression_Literal::kBoolean: {
            type = std::make_shared<DataTypeUInt8>();
            field = literal.boolean() ? UInt8(1) : UInt8(0);
            break;
        }
        case substrait::Expression_Literal::kI16: {
            type = std::make_shared<DataTypeInt16>();
            field = literal.i16();
            break;
        }
        case substrait::Expression_Literal::kI8: {
            type = std::make_shared<DataTypeInt8>();
            field = literal.i8();
            break;
        }
        case substrait::Expression_Literal::kDate: {
            type = std::make_shared<DataTypeDate32>();
            field = literal.date();
            break;
        }
        case substrait::Expression_Literal::kTimestamp: {
            type = std::make_shared<DataTypeDateTime64>(6);
            field = DecimalField<DateTime64>(literal.timestamp(), 6);
            break;
        }
        case substrait::Expression_Literal::kDecimal: {
            UInt32 precision = literal.decimal().precision();
            UInt32 scale = literal.decimal().scale();
            const auto & bytes = literal.decimal().value();

            if (precision <= DataTypeDecimal32::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal32>(precision, scale);
                auto value = *reinterpret_cast<const Int32 *>(bytes.data());
                field = DecimalField<Decimal32>(value, scale);
            }
            else if (precision <= DataTypeDecimal64::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal64>(precision, scale);
                auto value = *reinterpret_cast<const Int64 *>(bytes.data());
                field = DecimalField<Decimal64>(value, scale);
            }
            else if (precision <= DataTypeDecimal128::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal128>(precision, scale);
                auto value = *reinterpret_cast<const Int128 *>(bytes.data());
                field = DecimalField<Decimal128>(value, scale);
            }
            else
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
            break;
        }
        /// TODO(taiyang-li) Other type: Struct/Map/List
        case substrait::Expression_Literal::kList: {
            /// TODO(taiyang-li) Implement empty list
            if (literal.has_empty_list())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty list not support!");

            DataTypePtr first_type;
            std::tie(first_type, std::ignore) = parseLiteral(literal.list().values(0));

            size_t list_len = literal.list().values_size();
            Array array(list_len);
            for (size_t i = 0; i < list_len; ++i)
            {
                auto type_and_field = std::move(parseLiteral(literal.list().values(i)));
                if (!first_type->equals(*type_and_field.first))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Literal list type mismatch:{} and {}",
                        first_type->getName(),
                        type_and_field.first->getName());
                array[i] = std::move(type_and_field.second);
            }

            type = std::make_shared<DataTypeArray>(first_type);
            field = std::move(array);
            break;
        }
        case substrait::Expression_Literal::kNull: {
            type = parseType(literal.null());
            field = std::move(Field{});
            break;
        }
        default: {
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "Unsupported spark literal type {}", magic_enum::enum_name(literal.literal_type_case()));
        }
    }
    return std::make_pair(std::move(type), std::move(field));
}

const ActionsDAG::Node * SerializedPlanParser::parseArgument(ActionsDAGPtr action_dag, const substrait::Expression & rel)
{
    auto add_column = [&](const DataTypePtr & type, const Field & field) -> auto
    {
        return &action_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field))));
    };

    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DataTypePtr type;
            Field field;
            std::tie(type, field) = parseLiteral(rel.literal());
            return add_column(type, field);
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

            /// options should be non-empty and literals
            const auto & options = rel.singular_or_list().options();
            if (options.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty SingularOrList not supported");
            if (!options[0].has_literal())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            DataTypePtr elem_type;
            std::tie(elem_type, std::ignore) = parseLiteral(options[0].literal());

            size_t options_len = options.size();
            MutableColumnPtr elem_column = elem_type->createColumn();
            elem_column->reserve(options_len);
            for (size_t i = 0; i < options_len; ++i)
            {
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");

                auto type_and_field = std::move(parseLiteral(options[i].literal()));
                if (!elem_type->equals(*type_and_field.first))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        type_and_field.first->getName());

                elem_column->insert(type_and_field.second);
            }

            MutableColumns elem_columns;
            elem_columns.emplace_back(std::move(elem_column));

            auto name = getUniqueName("__set");
            Block elem_block;
            elem_block.insert(ColumnWithTypeAndName(nullptr, elem_type, name));
            elem_block.setColumns(std::move(elem_columns));

            SizeLimits limit;
            auto elem_set = std::make_shared<Set>(limit, true, false);
            elem_set->setHeader(elem_block.getColumnsWithTypeAndName());
            elem_set->insertFromBlock(elem_block.getColumnsWithTypeAndName());
            elem_set->finishInsert();

            auto arg = ColumnSet::create(elem_set->getTotalRowCount(), elem_set);
            args.emplace_back(&action_dag->addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), name)));

            const auto * function_node = toFunctionNode(action_dag, "in", args);
            action_dag->addOrReplaceInIndex(*function_node);
            return function_node;
        }

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

QueryPlanPtr SerializedPlanParser::parse(const std::string & plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    auto ok = plan_ptr->ParseFromString(plan);
    if (!ok)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Plan from string failed");

    auto res = std::move(parse(std::move(plan_ptr)));

    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    if (logger->debug())
    {
        auto out = PlanUtil::explainPlan(*res);
        LOG_DEBUG(logger, "clickhouse plan:{}", out);
    }
    return std::move(res);
}

QueryPlanPtr SerializedPlanParser::parseJson(const std::string & json_plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    google::protobuf::util::JsonStringToMessage(
        google::protobuf::stringpiece_internal::StringPiece(json_plan.c_str()),
        plan_ptr.get());
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
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI)
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
        auto actions_dag
            = parseFunction(query_plan->getCurrentDataStream().header, join.post_join_filter(), filter_name, useless, nullptr, true);
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

void SerializedPlanParser::wrapNullable(std::vector<String> columns, ActionsDAGPtr actionsDag,
                                        std::map<std::string, std::string>& nullable_measure_names)
{
    for (const auto & item : columns)
    {
        ActionsDAG::NodeRawConstPtrs args;
        args.emplace_back(&actionsDag->findInIndex(item));
        const auto * node = toFunctionNode(actionsDag, "toNullable", args);
        actionsDag->addOrReplaceInIndex(*node);
        nullable_measure_names[item] = node->result_name;
    }
}

DB::QueryPlanPtr SerializedPlanParser::parseSort(const substrait::SortRel & sort_rel)
{
    auto query_plan = parseOp(sort_rel.input());
    auto sort_descr = parseSortDescription(sort_rel);
    const auto & settings = context->getSettingsRef();
    auto sorting_step = std::make_unique<DB::SortingStep>(
        query_plan->getCurrentDataStream(),
        sort_descr,
        settings.max_block_size,
        0, // no limit now
        SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
        settings.max_bytes_before_remerge_sort,
        settings.remerge_sort_lowered_memory_bytes_ratio,
        settings.max_bytes_before_external_sort,
        context->getTemporaryVolume(),
        settings.min_free_disk_space_for_temporary_data);
    sorting_step->setStepDescription("Sorting step");
    query_plan->addStep(std::move(sorting_step));
    return std::move(query_plan);
}

DB::SortDescription SerializedPlanParser::parseSortDescription(const substrait::SortRel & sort_rel)
{
    static std::map<int, std::pair<int, int>> direction_map = {{1, {1, -1}}, {2, {1, 1}}, {3, {-1, 1}}, {4, {-1, -1}}};

    DB::SortDescription sort_descr;
    for (int i = 0, sz = sort_rel.sorts_size(); i < sz; ++i)
    {
        const auto & sort_field = sort_rel.sorts(i);

        if (!sort_field.expr().has_selection() || !sort_field.expr().selection().has_direct_reference()
            || !sort_field.expr().selection().direct_reference().has_struct_field())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupport sort field");
        }
        auto field_pos = sort_field.expr().selection().direct_reference().struct_field().field();

        auto direction_iter = direction_map.find(sort_field.direction());
        if (direction_iter == direction_map.end())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsuppor sort direction: {}", sort_field.direction());
        }

        sort_descr.emplace_back(field_pos, direction_iter->second.first, direction_iter->second.second);
    }
    return sort_descr;
}
SharedContextHolder SerializedPlanParser::shared_context;

void LocalExecutor::execute(QueryPlanPtr query_plan)
{
    current_query_plan = std::move(query_plan);
    Stopwatch stopwatch;
    stopwatch.start();
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = true};
    auto pipeline_builder = current_query_plan->buildQueryPipeline(
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
    this->header = current_query_plan->getCurrentDataStream().header.cloneEmpty();
    this->ch_column_to_spark_row = std::make_unique<CHColumnToSparkRow>();
}
std::unique_ptr<SparkRowInfo> LocalExecutor::writeBlockToSparkRow(Block & block)
{
    return this->ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}
bool LocalExecutor::hasNext()
{
    bool has_next;
    try
    {
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
    }
    catch (DB::Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("LocalExecutor"), "run query plan failed. {}\n{}", e.message(), PlanUtil::explainPlan(*current_query_plan));
        throw e;
    }
    return has_next;
}
SparkRowInfoPtr LocalExecutor::next()
{
    checkNextValid();
    SparkRowInfoPtr row_info = writeBlockToSparkRow(currentBlock());
    consume();
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
LocalExecutor::LocalExecutor(QueryContext & _query_context)
    : query_context(_query_context)
{
}
}
