#include "SerializedPlanParser.h"
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBlockOutputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <sys/stat.h>
#include <Poco/URI.h>
#include <Common/MergeTreeTool.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Storages/ParquetRowInputFormat.h>
#include <Poco/Util/MapConfiguration.h>
#include <base/logger_useful.h>

bool local_engine::SerializedPlanParser::isReadRelFromJava(const substrait::ReadRel& rel) {
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    return rel.local_files().items().size() == 1 && rel.local_files().items().at(0).uri_file().starts_with("iterator");
}


DB::QueryPlanPtr local_engine::SerializedPlanParser::parseReadRealWithLocalFile(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    auto files_info = std::make_shared<FilesInfo>();
    for (const auto & item : rel.local_files().items())
    {
        Poco::URI uri(item.uri_file());
        files_info->files.push_back(uri.getPath());
    }
    auto query_plan = std::make_unique<DB::QueryPlan>();
    std::shared_ptr<IProcessor> source = std::make_shared<BatchParquetFileSource>(files_info, parseNameStruct(rel.base_schema()));
    auto source_step = std::make_unique<ReadFromStorageStep>(Pipe(source), "Parquet");
    query_plan->addStep(std::move(source_step));
    return query_plan;
}

DB::QueryPlanPtr local_engine::SerializedPlanParser::parseReadRealWithJavaIter(const substrait::ReadRel& rel){
    assert(rel.has_local_files());
    assert(rel.local_files().items().size() == 1);
    assert(rel.has_base_schema());
    auto iter = rel.local_files().items().at(0).uri_file();
    auto pos = iter.find(':');
    auto iter_index = std::stoi(iter.substr(pos+1, iter.size()));
    auto plan = std::make_unique<DB::QueryPlan>();
    auto source = std::make_shared<local_engine::SourceFromJavaIter>(parseNameStruct(rel.base_schema()), input_iters[iter_index], vm);
    DB::QueryPlanStepPtr source_step = std::make_unique<DB::ReadFromPreparedSource>(Pipe(source), context);
    plan->addStep(std::move(source_step));
    return plan;
}

DB::QueryPlanPtr local_engine::SerializedPlanParser::parseMergeTreeTable(const substrait::ReadRel& rel)
{
    Stopwatch watch;
    watch.start();
    assert(rel.has_extension_table());
    assert(rel.has_base_schema());
    std::string table = rel.extension_table().detail().value();
    auto merge_tree_table = local_engine::parseMergeTreeTable(table);
    auto header = parseNameStruct(rel.base_schema());
    auto names_and_types_list = header.getNamesAndTypesList();
    auto storageFactory = local_engine::StorageMergeTreeFactory::instance();
//    auto metadata = storageFactory.getMetadata(DB::StorageID(merge_tree_table.database, merge_tree_table.table), [names_and_types_list, this]()->local_engine::StorageInMemoryMetadataPtr {
//        return local_engine::buildMetaData(names_and_types_list, this->context);
//    });
    auto metadata = local_engine::buildMetaData(names_and_types_list, this->context);
    auto t_metadata = watch.elapsedMicroseconds();
    query_context.metadata = metadata;
    auto storage = storageFactory.getStorage(DB::StorageID(merge_tree_table.database, merge_tree_table.table), metadata->getColumns(), [merge_tree_table, metadata]() -> local_engine::CustomStorageMergeTreePtr {
                auto  custom_storage_merge_tree = std::make_shared<local_engine::CustomStorageMergeTree>(
                    DB::StorageID(merge_tree_table.database, merge_tree_table.table),
                    merge_tree_table.relative_path,
                    *metadata,
                    false,
                    global_context,
                    "",
                    DB::MergeTreeData::MergingParams(),
                    local_engine::buildMergeTreeSettings());
                custom_storage_merge_tree->loadDataParts(false);
                return custom_storage_merge_tree;
           });
    query_context.storage_snapshot = std::make_shared<StorageSnapshot>(*storage, metadata);
    auto t_storage =  watch.elapsedMicroseconds() - t_metadata;
    query_context.custom_storage_merge_tree = storage;
    auto query_info = local_engine::buildQueryInfo(names_and_types_list);
    auto data_parts = query_context.custom_storage_merge_tree->getDataPartsVector();
    int min_block = merge_tree_table.min_block;
    int max_block = merge_tree_table.max_block;
    MergeTreeData::DataPartsVector selected_parts;
    std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                 [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block < max_block;});
    if (selected_parts.empty())
    {
        LOG_ERROR(&Poco::Logger::get("SerializedPlanParser"), "part {} not found.", min_block);
        throw std::exception();
    }
    auto query = query_context.custom_storage_merge_tree->reader.readFromParts(selected_parts,
                                                        names_and_types_list.getNames(),
                                                        query_context.storage_snapshot,
                                                        *query_info,
                                                        this->context,
                                                        4096 * 2,
                                                        1);
    auto t_pipe =  watch.elapsedMicroseconds() - t_storage;
    watch.stop();
    LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"),
             "get metadata {} ms; get storage {} ms; get pipe {} ms",
             t_metadata / 1000.0,
             t_storage / 1000.0,
             t_pipe / 1000.0);
    return query;
}


DB::Block local_engine::SerializedPlanParser::parseNameStruct(const substrait::NamedStruct & struct_)
{
    auto internal_cols = std::make_unique<std::vector<DB::ColumnWithTypeAndName>>();
    internal_cols->reserve(struct_.names_size());
    for (int i = 0; i < struct_.names_size(); ++i)
    {
        const auto & name = struct_.names(i);
        const auto & type = struct_.struct_().types(i);
        auto data_type = parseType(type);
        internal_cols->push_back(DB::ColumnWithTypeAndName(data_type->createColumn(), data_type, name));
    }
    return DB::Block(*std::move(internal_cols));
}
DB::DataTypePtr local_engine::SerializedPlanParser::parseType(const substrait::Type & type)
{
    auto & factory = DB::DataTypeFactory::instance();
    if (type.has_bool_() || type.has_i8())
    {
        return factory.get("Int8");
    }
    else if (type.has_i16())
    {
        return factory.get("Int16");
    }
    else if (type.has_i32())
    {
        return factory.get("Int32");
    }
    else if (type.has_i64())
    {
        return factory.get("Int64");
    }
    else if (type.has_string())
    {
        return factory.get("String");
    }
    else if (type.has_fp32())
    {
        return factory.get("Float32");
    }
    else if (type.has_fp64())
    {
        return factory.get("Float64");
    }
    else if (type.has_date())
    {
        return factory.get("Date");
    }
    else
    {
        throw std::runtime_error("doesn't support type " + type.DebugString());
    }
}
DB::QueryPlanPtr local_engine::SerializedPlanParser::parse(std::unique_ptr<substrait::Plan> plan)
{
    if (plan->extensions_size() > 0)
    {
        for (const auto& extension : plan->extensions())
        {
            if (extension.has_extension_function())
            {
                this->function_mapping.emplace(std::to_string(extension.extension_function().function_anchor()), extension.extension_function().name());
            }
        }
    }
    if (plan->relations_size() == 1)
    {

        auto root_rel = plan->relations().at(0);
        assert(root_rel.has_root() && "must have root rel!");
        return parseOp(root_rel.root().input());
    }
    else
    {
        throw std::runtime_error("too many relations found");
    }
}

DB::QueryPlanPtr local_engine::SerializedPlanParser::parseOp(const substrait::Rel & rel)
{
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kFetch:
        {
            const auto & limit = rel.fetch();
            DB::QueryPlanPtr query_plan = parseOp(limit.input());
            auto limit_step = std::make_unique<DB::LimitStep>(query_plan->getCurrentDataStream(), limit.count(), limit.offset());
            query_plan->addStep(std::move(limit_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kFilter:
        {
            const auto & filter = rel.filter();
            DB::QueryPlanPtr query_plan = parseOp(filter.input());
            std::string filter_name;
            auto actions_dag = parseFunction(query_plan->getCurrentDataStream(), filter.condition(), filter_name, nullptr, true);
//            actions_dag->removeUnusedActions(query_plan->getCurrentDataStream().header.getNames());
            auto filter_step = std::make_unique<DB::FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
            query_plan->addStep(std::move(filter_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kProject:
        {
            const auto & project = rel.project();
            DB::QueryPlanPtr query_plan = parseOp(project.input());
            const auto & expressions = project.expressions();
            auto actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
            DB::NamesWithAliases required_columns;
            for (const auto & expr : expressions)
            {
                if (expr.has_selection())
                {
                    const auto * field = actions_dag->getInputs()[expr.selection().direct_reference().struct_field().field()];
                    required_columns.emplace_back(DB::NameWithAlias (field->result_name, field->result_name));
                }
                else if (expr.has_scalar_function())
                {
                    std::string name;
                    actions_dag = parseFunction(query_plan->getCurrentDataStream(), expr, name, actions_dag, true);
                    if (!name.empty()) {

                    }
                    required_columns.emplace_back(DB::NameWithAlias (name, name));
                }
                else if (expr.has_literal())
                {
                    auto const_col = parseArgument(actions_dag, expr);
                    actions_dag->addOrReplaceInIndex(*const_col);
                    required_columns.emplace_back(DB::NameWithAlias(const_col->result_name, const_col->result_name));
                }
                else
                {
                    throw std::runtime_error("unsupported projection type");
                }
            }
            actions_dag->project(required_columns);
            auto expression_step = std::make_unique<DB::ExpressionStep>(
                query_plan->getCurrentDataStream(), actions_dag);
            query_plan->addStep(std::move(expression_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kAggregate:
        {
            const auto & aggregate = rel.aggregate();
            DB::QueryPlanPtr query_plan = parseOp(aggregate.input());
            auto aggregate_step = parseAggregate(*query_plan, aggregate);
            query_plan->addStep(std::move(aggregate_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kRead:
        {
            const auto & read = rel.read();
            assert(read.has_local_files() || read.has_extension_table() && "Only support local parquet files or merge tree read rel");
            DB::QueryPlanPtr query_plan;
            if (read.has_local_files())
            {
                if(isReadRelFromJava(read))
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
            return query_plan;
        }
        default:
            throw std::runtime_error("doesn't support relation type " + std::to_string(rel.rel_type_case()));
    }
}

DB::AggregateFunctionPtr getAggregateFunction(const std::string & name, DB::DataTypes arg_types)
{
    auto & factory = DB::AggregateFunctionFactory::instance();
    DB::AggregateFunctionProperties properties;
    return factory.get(name, arg_types, DB::Array{}, properties);
}

DB::QueryPlanStepPtr local_engine::SerializedPlanParser::parseAggregate(DB::QueryPlan & plan, const substrait::AggregateRel & rel)
{
    auto input = plan.getCurrentDataStream();
    DB::ActionsDAGPtr expression = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input.header));
    std::vector<std::string> measure_names;
    for (const auto& measure : rel.measures())
    {
        assert(measure.measure().args_size() == 1 && "only support one argument aggregate function");
        auto arg = measure.measure().args(0);
        if (arg.has_scalar_function()) {
            std::string name;
            parseFunction(input, arg, name, expression, true);
            measure_names.emplace_back(name);
        }
        else if (arg.has_selection())
        {
            auto name = input.header.getByPosition(arg.selection().direct_reference().struct_field().field()).name;
            measure_names.emplace_back(name);
        }
        else
        {
            throw std::runtime_error("unsupported aggregate argument type.");
        }
    }
    auto expression_before_aggregate = std::make_unique<DB::ExpressionStep>(input, expression);
    plan.addStep(std::move(expression_before_aggregate));

    std::set<substrait::AggregationPhase> phase_set;
    for (int i = 0; i < rel.measures_size(); ++i)
    {
        const auto & measure = rel.measures(i);
        phase_set.emplace(measure.measure().phase());
    }
    if (phase_set.size() > 1)
    {
        throw std::runtime_error("two many aggregate phase!");
    }
    bool final=true;
    if (phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
        || phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE))
        final = false;

    bool only_merge = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);


    DB::ColumnNumbers keys = {};
    if (rel.groupings_size() == 1)
    {
        for (auto group : rel.groupings(0).grouping_expressions())
        {
            if (group.has_selection() && group.selection().has_direct_reference())
            {
                keys.emplace_back(group.selection().direct_reference().struct_field().field());
            }
            else
            {
                throw std::runtime_error("unsupported group expression");

            }
        }
    }
    // only support one grouping or no grouping
    else if (rel.groupings_size() != 0)
    {
        throw std::runtime_error("too many groupings");
    }

    auto aggregates = DB::AggregateDescriptions();
    for (int i = 0; i < rel.measures_size(); ++i)
    {
        const auto& measure = rel.measures(i);
        DB::AggregateDescription agg;
        auto function_signature = this->function_mapping.at(std::to_string(measure.measure().function_reference()));
        auto function_name_idx = function_signature.find(":");
        assert(function_name_idx != function_signature.npos && ("invalid function signature: " + function_signature).c_str());
        auto function_name = function_signature.substr(0, function_name_idx);
        if (only_merge)
        {
            agg.column_name = measure_names.at(i);
        }
        else
        {
            agg.column_name = function_name +"(" + measure_names.at(i) + ")";
        }
        agg.arguments = DB::ColumnNumbers{plan.getCurrentDataStream().header.getPositionByName(measure_names.at(i))};
        agg.argument_names = DB::Names{measure_names.at(i)};
        agg.function = ::getAggregateFunction(function_name, {plan.getCurrentDataStream().header.getByName(measure_names.at(i)).type});
        aggregates.push_back(agg);
    }


    if (only_merge)
    {
        auto transform_params = std::make_shared<AggregatingTransformParams>(this->getMergedAggregateParam(plan.getCurrentDataStream().header, keys, aggregates), final);
        return std::make_unique<DB::MergingAggregatedStep>(
            plan.getCurrentDataStream(),
            transform_params,
            false,
            1,
            1);
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
            DB::SortDescription());
        return aggregating_step;
    }
}

DB::NamesAndTypesList local_engine::SerializedPlanParser::blockToNameAndTypeList(const DB::Block & header)
{
    DB::NamesAndTypesList types;
    for (const auto & name : header.getNames())
    {
        const auto * column = header.findByName(name);
        types.push_back(DB::NameAndTypePair(column->name, column->type));
    }
    return types;
}

void join(DB::ActionsDAG::NodeRawConstPtrs v, char c, std::string & s)
{
    s.clear();
    for (auto p = v.begin(); p != v.end(); ++p)
    {
        s += (*p)->result_name;
        if (p != v.end() - 1)
            s += c;
    }
}

std::string local_engine::SerializedPlanParser::getFunctionName(std::string function_signature, const substrait::Type& output_type)
{
    auto function_name_idx = function_signature.find(":");
    assert(function_name_idx != function_signature.npos && ("invalid function signature: " + function_signature).c_str());
    auto function_name = function_signature.substr(0, function_name_idx);
    if (!SCALAR_FUNCTIONS.contains(function_name))
    {
        throw std::runtime_error("doesn't support function " + function_name);
    }
    std::string ch_function_name;
    if (function_name == "cast")
    {
        if (output_type.has_fp64())
        {
            ch_function_name = "toFloat64";
        }
        else if (output_type.has_string())
        {
            ch_function_name = "toString";
        }
        else
        {
            LOG_ERROR(&Poco::Logger::get("SerializedPlanParser"), "doesn't support function {}", function_signature);
            throw std::runtime_error("doesn't support function " + function_signature);
        }
    }
    else
    {
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
    }
    return ch_function_name;
}

DB::ActionsDAGPtr local_engine::SerializedPlanParser::parseFunction(
    const DataStream & input, const substrait::Expression & rel, std::string & result_name, DB::ActionsDAGPtr actions_dag, bool keep_result)
{
    assert(rel.has_scalar_function() && "the root of expression should be a scalar function");
    const auto & scalar_function = rel.scalar_function();
    if (!actions_dag)
    {
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input.header));
    }
    auto function_signature = this->function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = getFunctionName(function_signature, rel.scalar_function().output_type());
    DB::ActionsDAG::NodeRawConstPtrs args;
    for (const auto & arg : scalar_function.args())
    {
        if (arg.has_scalar_function())
        {
            std::string arg_name;
            bool keep_arg = local_engine::FUNCTION_NEED_KEEP_ARGUMENTS.contains(function_name);
            parseFunction(input, arg, arg_name, actions_dag, keep_arg);
            args.emplace_back(&actions_dag->getNodes().back());
        }
        else
        {
            args.emplace_back(parseArgument(actions_dag, arg));
        }
    }
    if (function_name == "alias")
    {
        result_name = args[0]->result_name;
        actions_dag->addAlias(actions_dag->findInIndex(result_name), result_name);
    } else {
        auto function_builder = DB::FunctionFactory::instance().get(function_name, this->context);
        std::string args_name;
        join(args, ',', args_name);
        result_name = function_name + "(" + args_name + ")";
        const auto * function_node = &actions_dag->addFunction(function_builder, args, result_name);
        if (keep_result)
            actions_dag->addOrReplaceInIndex(*function_node);
    }
    return actions_dag;
}

const DB::ActionsDAG::Node * local_engine::SerializedPlanParser::
    parseArgument(DB::ActionsDAGPtr action_dag, const substrait::Expression & rel)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral:
        {
            const auto & literal = rel.literal();
            switch (literal.literal_type_case())
            {
                case substrait::Expression_Literal::kFp64:
                {
                    auto type = std::make_shared<DB::DataTypeFloat64>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.fp64()), type, getUniqueName(std::to_string(literal.fp64()))));
                }
                case substrait::Expression_Literal::kString:
                {
                    auto type = std::make_shared<DB::DataTypeString>();
                    return &action_dag->addColumn(
                        ColumnWithTypeAndName(type->createColumnConst(1, literal.string()), type, getUniqueName(literal.string())));
                }
                case substrait::Expression_Literal::kI32:
                {
                    auto type = std::make_shared<DB::DataTypeInt32>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.i32()), type, getUniqueName(std::to_string(literal.i32()))));
                }
                case substrait::Expression_Literal::kDate:
                {

                    auto type = std::make_shared<DB::DataTypeDate>();
                    return &action_dag->addColumn(ColumnWithTypeAndName(
                        type->createColumnConst(1, literal.date()), type, getUniqueName(std::to_string(literal.date()))));
                }
                default:
                    throw std::runtime_error("unsupported constant type " + std::to_string(literal.literal_type_case()));
            }
        }
        case substrait::Expression::RexTypeCase::kSelection:
        {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
            {
                throw std::runtime_error("Can only have direct struct references in selections");
            }
            const auto * field = action_dag->getInputs()[rel.selection().direct_reference().struct_field().field()];
            return action_dag->tryFindInIndex(field->result_name);
        }
        default:
            throw std::runtime_error("unsupported arg type " + std::to_string(rel.rex_type_case()));
    }
}


DB::QueryPlanPtr local_engine::SerializedPlanParser::parse(std::string & plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    plan_ptr->ParseFromString(plan);
//    std::cerr << plan_ptr->DebugString() <<std::endl;
    return parse(std::move(plan_ptr));
}
void local_engine::SerializedPlanParser::initFunctionEnv()
{
    DB::registerFunctions();
    DB::registerAggregateFunctions();
}
local_engine::SerializedPlanParser::SerializedPlanParser(const DB::ContextPtr & context) : context(context)
{
}
local_engine::ContextMutablePtr local_engine::SerializedPlanParser::global_context = nullptr;

Context::ConfigurationPtr local_engine::SerializedPlanParser::config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

SharedContextHolder local_engine::SerializedPlanParser::shared_context;

DB::Chunk local_engine::BatchParquetFileSource::generate()
{
    while (!finished_generate)
    {
        /// Open file lazily on first read. This is needed to avoid too many open files from different streams.
        if (!reader)
        {
            auto current_file = files_info->next_file_to_read.fetch_add(1);
            if (current_file >= files_info->files.size())
                return {};

            current_path = files_info->files[current_file];
            std::unique_ptr<ReadBuffer> nested_buffer;

            struct stat file_stat
            {
            };

            /// Check if file descriptor allows random reads (and reading it twice).
            if (0 != stat(current_path.c_str(), &file_stat))
                throw std::runtime_error("Cannot stat file " + current_path);

            if (S_ISREG(file_stat.st_mode))
                nested_buffer = std::make_unique<ReadBufferFromFilePRead>(current_path);
            else
                nested_buffer = std::make_unique<ReadBufferFromFile>(current_path);


            read_buf = std::move(nested_buffer);
            ProcessorPtr format = std::make_shared<local_engine::ParquetRowInputFormat>(*read_buf, header);
//            auto format = DB::ParquetBlockInputFormat::getParquetFormat(*read_buf, header);
            QueryPipelineBuilder builder;
            builder.init(Pipe(format));
            pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

            reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            return chunk;
        }

        finished_generate = true;

        /// Close file prematurely if stream was ended.
        reader.reset();
        pipeline.reset();
        read_buf.reset();
    }

    return {};
}
local_engine::BatchParquetFileSource::BatchParquetFileSource(FilesInfoPtr files, const DB::Block & sample)
    : SourceWithProgress(sample), files_info(files), header(sample)
{
}
void local_engine::LocalExecutor::execute(DB::QueryPlanPtr query_plan)
{
    Stopwatch stopwatch;
    stopwatch.start();
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = true};
    auto pipeline_builder = query_plan->buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings{
                                                                                     .actions_settings = ExpressionActionsSettings{
                                                                                         .can_compile_expressions = true,
                                                                                         .min_count_to_compile_expression = 3,
                                                                                     .compile_expressions = CompileExpressions::yes
                                                                                    }});
    this->query_pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    auto t_pipeline = stopwatch.elapsedMicroseconds();
    this->executor = std::make_unique<DB::PullingPipelineExecutor>(query_pipeline);
    auto t_executor = stopwatch.elapsedMicroseconds() - t_pipeline;
    stopwatch.stop();
    LOG_INFO(&Poco::Logger::get("SerializedPlanParser"),
             "build pipeline {} ms; create executor {} ms;",
             t_pipeline / 1000.0,
             t_executor / 1000.0);
    this->header = query_plan->getCurrentDataStream().header.cloneEmpty();
    this->ch_column_to_spark_row = std::make_unique<local_engine::CHColumnToSparkRow>();
}
std::unique_ptr<local_engine::SparkRowInfo> local_engine::LocalExecutor::writeBlockToSparkRow(DB::Block & block)
{
    return this->ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}
bool local_engine::LocalExecutor::hasNext()
{
    bool has_next;
    if (!this->current_chunk || this->current_chunk->rows() == 0)
    {
        this->current_chunk = std::make_unique<DB::Block>(this->header);
        has_next = this->executor->pull(*this->current_chunk);
    }
    else
    {
        has_next = true;
    }
    return has_next;
}
local_engine::SparkRowInfoPtr local_engine::LocalExecutor::next()
{
    local_engine::SparkRowInfoPtr row_info = writeBlockToSparkRow(*this->current_chunk);
    this->current_chunk.reset();
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

Block* local_engine::LocalExecutor::nextColumnar()
{
    Block * columnar_batch;
    if (this->current_chunk->columns() > 0)
    {
        columnar_batch = new Block(*this->current_chunk);
        this->current_chunk.reset();
    }
    else
    {
        columnar_batch = new Block(header.cloneEmpty());
        this->current_chunk.reset();
    }
    return columnar_batch;
}

DB::Block & local_engine::LocalExecutor::getHeader()
{
    return header;
}
local_engine::LocalExecutor::LocalExecutor(local_engine::QueryContext & _query_context):query_context(_query_context)
{
}
