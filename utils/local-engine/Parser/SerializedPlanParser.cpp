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
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <sys/stat.h>
#include <Poco/URI.h>
#include <Storages/MergeTreeTool.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Poco/Util/MapConfiguration.h>



DB::BatchParquetFileSourcePtr dbms::SerializedPlanParser::parseReadRealWithLocalFile(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    auto files_info = std::make_shared<FilesInfo>();
    for (const auto & item : rel.local_files().items())
    {
        Poco::URI uri(item.uri_file());
        files_info->files.push_back(uri.getPath());
    }
    return std::make_shared<BatchParquetFileSource>(files_info, parseNameStruct(rel.base_schema()));
}

DB::QueryPlanPtr dbms::SerializedPlanParser::parseMergeTreeTable(const substrait::ReadRel& rel)
{
    assert(rel.has_extension_table());
    assert(rel.has_base_schema());
    std::string table = rel.extension_table().detail().value();
    auto merge_tree_table = local_engine::parseMergeTreeTable(table);
    auto header = parseNameStruct(rel.base_schema());
    auto names_and_types_list = header.getNamesAndTypesList();
    auto factory = local_engine::StorageMergeTreeFactory::instance();

    auto metadata = local_engine::buildMetaData(names_and_types_list, this->context);
    query_context.metadata = metadata;
    auto storage = factory.getStorage(DB::StorageID(merge_tree_table.database, merge_tree_table.table), [merge_tree_table, metadata]() -> local_engine::CustomStorageMergeTreePtr {
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
    query_context.custom_storage_merge_tree = storage;
    auto query_info = local_engine::buildQueryInfo(names_and_types_list);
    auto data_parts = query_context.custom_storage_merge_tree->getDataPartsVector();
    int min_block = merge_tree_table.min_block;
    int max_block = merge_tree_table.max_block;
    MergeTreeData::DataPartsVector selected_parts;
    std::copy_if(std::begin(data_parts), std::end(data_parts), std::inserter(selected_parts, std::begin(selected_parts)),
                 [min_block, max_block](MergeTreeData::DataPartPtr part) { return part->info.min_block>=min_block && part->info.max_block < max_block;});
    auto query = query_context.custom_storage_merge_tree->reader.readFromParts(selected_parts,
                                                        names_and_types_list.getNames(),
                                                        query_context.metadata,
                                                        query_context.metadata,
                                                        *query_info,
                                                        this->context,
                                                        4096 * 2,
                                                        1);
    return query;
}


DB::Block dbms::SerializedPlanParser::parseNameStruct(const substrait::NamedStruct & struct_)
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
DB::DataTypePtr dbms::SerializedPlanParser::parseType(const substrait::Type & type)
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
DB::QueryPlanPtr dbms::SerializedPlanParser::parse(std::unique_ptr<substrait::Plan> plan)
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

DB::QueryPlanPtr dbms::SerializedPlanParser::parseOp(const substrait::Rel & rel)
{
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kFetch: {
            const auto & limit = rel.fetch();
            DB::QueryPlanPtr query_plan = parseOp(limit.input());
            auto limit_step = std::make_unique<DB::LimitStep>(query_plan->getCurrentDataStream(), limit.count(), limit.offset());
            query_plan->addStep(std::move(limit_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kFilter: {
            const auto & filter = rel.filter();
            DB::QueryPlanPtr query_plan = parseOp(filter.input());
            std::string filter_name;
            auto actions_dag = parseFunction(query_plan->getCurrentDataStream(), filter.condition(), filter_name, nullptr, true);
            auto filter_step = std::make_unique<DB::FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
            query_plan->addStep(std::move(filter_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kProject: {
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
        case substrait::Rel::RelTypeCase::kAggregate: {
            const auto & aggregate = rel.aggregate();
            DB::QueryPlanPtr query_plan = parseOp(aggregate.input());
            auto aggregate_step = parseAggregate(*query_plan, aggregate);
            query_plan->addStep(std::move(aggregate_step));
            return query_plan;
        }
        case substrait::Rel::RelTypeCase::kRead: {
            const auto & read = rel.read();
            assert(read.has_local_files() || read.has_extension_table() && "Only support local parquet files or merge tree read rel");
            DB::QueryPlanPtr query_plan;
            if (read.has_local_files())
            {
                query_plan = std::make_unique<DB::QueryPlan>();
                std::shared_ptr<IProcessor> source = std::dynamic_pointer_cast<IProcessor>(parseReadRealWithLocalFile(read));
                auto source_step = std::make_unique<ReadFromStorageStep>(Pipe(source), "Parquet");
                query_plan->addStep(std::move(source_step));
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

DB::QueryPlanStepPtr dbms::SerializedPlanParser::parseAggregate(DB::QueryPlan & plan, const substrait::AggregateRel & rel)
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

    // TODO need support grouping key
    auto aggregates = DB::AggregateDescriptions();
    for (int i = 0; i < rel.measures_size(); ++i)
    {
        const auto& measure = rel.measures(i);
        DB::AggregateDescription agg;
        auto function_name = this->function_mapping.at(std::to_string(measure.measure().function_reference()));
        agg.column_name = function_name +"(" + measure_names.at(i) + ")";
        agg.arguments = DB::ColumnNumbers{plan.getCurrentDataStream().header.getPositionByName(measure_names.at(i))};
        agg.argument_names = DB::Names{measure_names.at(i)};
        agg.function = ::getAggregateFunction(function_name, {plan.getCurrentDataStream().header.getByName(measure_names.at(i)).type});
        aggregates.push_back(agg);
    }

    auto aggregating_step = std::make_unique<AggregatingStep>(
        plan.getCurrentDataStream(),
        this->getAggregateParam(plan.getCurrentDataStream().header, {}, aggregates),
        true,
        1000000,
        1,
        1,
        false,
        nullptr,
        DB::SortDescription());
    return aggregating_step;
}

DB::NamesAndTypesList dbms::SerializedPlanParser::blockToNameAndTypeList(const DB::Block & header)
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


DB::ActionsDAGPtr dbms::SerializedPlanParser::parseFunction(
    const DataStream & input, const substrait::Expression & rel, std::string & result_name, DB::ActionsDAGPtr actions_dag, bool keep_result)
{
    assert(rel.has_scalar_function() && "the root of expression should be a scalar function");
    const auto & scalar_function = rel.scalar_function();
    if (!actions_dag)
    {
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input.header));
    }
    DB::ActionsDAG::NodeRawConstPtrs args;
    for (const auto & arg : scalar_function.args())
    {
        if (arg.has_scalar_function())
        {
            std::string arg_name;
            parseFunction(input, arg, arg_name, actions_dag);
            args.emplace_back(&actions_dag->getNodes().back());
        }
        else
        {
            args.emplace_back(parseArgument(actions_dag, arg));
        }
    }
    auto function_name = this->function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    assert(SCALAR_FUNCTIONS.contains(function_name) && ("doesn't support function " + function_name).c_str());
    auto function_builder = DB::FunctionFactory::instance().get(SCALAR_FUNCTIONS.at(function_name), this->context);
    std::string args_name;
    join(args, ',', args_name);
    result_name = function_name + "(" + args_name + ")";
    const auto* function_node = &actions_dag->addFunction(function_builder, args, result_name);
    if (keep_result)
        actions_dag->addOrReplaceInIndex(*function_node);
    return actions_dag;
}

const DB::ActionsDAG::Node * dbms::SerializedPlanParser::parseArgument(DB::ActionsDAGPtr action_dag, const substrait::Expression & rel)
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


DB::QueryPlanPtr dbms::SerializedPlanParser::parse(std::string & plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    plan_ptr->ParseFromString(plan);
    return parse(std::move(plan_ptr));
}
void dbms::SerializedPlanParser::initFunctionEnv()
{
    dbms::registerFunctions();
    dbms::registerAggregateFunctions();
}
dbms::SerializedPlanParser::SerializedPlanParser(const DB::ContextPtr & context) : context(context)
{
}
dbms::ContextMutablePtr dbms::SerializedPlanParser::global_context = nullptr;

Context::ConfigurationPtr dbms::SerializedPlanParser::config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

SharedContextHolder dbms::SerializedPlanParser::shared_context;

DB::Chunk DB::BatchParquetFileSource::generate()
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
//            ProcessorPtr format = std::make_shared<local_engine::ParquetRowInputFormat>(*read_buf, header);
            auto format = DB::ParquetBlockInputFormat::getParquetFormat(*read_buf, header);
            pipeline = std::make_unique<QueryPipeline>();
            pipeline->init(Pipe(format));

            reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
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
DB::BatchParquetFileSource::BatchParquetFileSource(FilesInfoPtr files, const DB::Block & sample)
    : SourceWithProgress(sample), files_info(files), header(sample)
{
}
void dbms::LocalExecutor::execute(DB::QueryPlanPtr query_plan)
{
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
    this->query_pipeline = query_plan->buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings{
                                                                                     .actions_settings = ExpressionActionsSettings{
                                                                                         .can_compile_expressions = true,
                                                                                         .min_count_to_compile_expression = 3,
                                                                                     .compile_expressions = CompileExpressions::yes
                                                                                    }});
    this->executor = std::make_unique<DB::PullingPipelineExecutor>(*query_pipeline);
    this->header = query_plan->getCurrentDataStream().header;
    this->ch_column_to_spark_row = std::make_unique<local_engine::CHColumnToSparkRow>();
}
std::unique_ptr<local_engine::SparkRowInfo> dbms::LocalExecutor::writeBlockToSparkRow(DB::Block & block)
{
    return this->ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}
bool dbms::LocalExecutor::hasNext()
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
local_engine::SparkRowInfoPtr dbms::LocalExecutor::next()
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
DB::Block & dbms::LocalExecutor::getHeader()
{
    return header;
}
dbms::LocalExecutor::LocalExecutor(dbms::QueryContext & _query_context):query_context(_query_context)
{
}
dbms::LocalExecutor::LocalExecutor()
{
}
