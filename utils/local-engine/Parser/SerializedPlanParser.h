#pragma once

#include <substrait/plan.pb.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <arrow/ipc/writer.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/SourceFromJavaIter.h>
#include <Parser/CHColumnToSparkRow.h>

namespace local_engine
{

static const std::map<std::string, std::string> SCALAR_FUNCTIONS = {
    {"is_not_null","isNotNull"},
    {"gte","greaterOrEquals"},
    {"gt", "greater"},
    {"lte", "lessOrEquals"},
    {"lt", "less"},
    {"equal", "equals"},

    {"and", "and"},
    {"or", "or"},
    {"not", "not"},
    {"xor", "xor"},

    {"TO_DATE", "toDate"},
    {"extract", ""},
    {"cast", ""},
    {"alias", "alias"},

    {"subtract", "minus"},
    {"multiply", "multiply"},
    {"add", "plus"},
    {"divide", "divide"},
    {"modulus", "modulo"},

    {"like", "like"},
    {"not_like", "notLike"},
    {"starts_with", "startsWith"},
    {"ends_with", "endsWith"},
    {"contains", "countSubstrings"},
    {"substring", "substring"},

    {"in", "in"},

    // aggregate functions
    {"count", "count"},
    {"avg", "avg"},
    {"sum", "sum"},
    {"min", "min"},
    {"max", "max"}
};

static const std::set<std::string> FUNCTION_NEED_KEEP_ARGUMENTS = {"alias"};

struct QueryContext {
    StorageSnapshotPtr storage_snapshot;
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata;
    std::shared_ptr<CustomStorageMergeTree> custom_storage_merge_tree;
};

class SerializedPlanParser
{
public:
    explicit SerializedPlanParser(const ContextPtr & context);
    static void initFunctionEnv();
    DB::QueryPlanPtr parse(std::string& plan);
    DB::QueryPlanPtr parse(std::unique_ptr<substrait::Plan> plan);

    DB::QueryPlanPtr parseReadRealWithLocalFile(const substrait::ReadRel& rel);
    DB::QueryPlanPtr parseReadRealWithJavaIter(const substrait::ReadRel& rel);
    DB::QueryPlanPtr parseMergeTreeTable(const substrait::ReadRel& rel);
    bool isReadRelFromJava(const substrait::ReadRel& rel);
    static DB::Block parseNameStruct(const substrait::NamedStruct& struct_);
    static DB::DataTypePtr parseType(const substrait::Type& type);

    void addInputIter(jobject iter)
    {
        input_iters.emplace_back(iter);
    }

    void setJavaVM(JavaVM * vm_)
    {
        vm = vm_;
    }

    static ContextMutablePtr global_context;
    static Context::ConfigurationPtr config;
    static SharedContextHolder shared_context;
    QueryContext query_context;

private:
    static DB::NamesAndTypesList blockToNameAndTypeList(const DB::Block & header);
    DB::QueryPlanPtr parseOp(const substrait::Rel &rel);
    DB::QueryPlanPtr parseJoin(substrait::JoinRel join, DB::QueryPlanPtr left, DB::QueryPlanPtr right);
    void reorderJoinOutput(DB::QueryPlan & plan, DB::Names cols);
    std::string getFunctionName(std::string function_sig, const substrait::Expression_ScalarFunction & function);
    DB::ActionsDAGPtr parseFunction(const DataStream & input, const substrait::Expression &rel, std::string & result_name, DB::ActionsDAGPtr actions_dag = nullptr, bool keep_result = false);
    const ActionsDAG::Node * parseFunctionWithDAG(const substrait::Expression &rel, std::string & result_name, DB::ActionsDAGPtr actions_dag = nullptr, bool keep_result = false);
    DB::QueryPlanStepPtr parseAggregate(DB::QueryPlan & plan, const substrait::AggregateRel &rel);
    const DB::ActionsDAG::Node * parseArgument(DB::ActionsDAGPtr action_dag, const substrait::Expression &rel);
    std::string getUniqueName(std::string name)
    {
        return name + "_" + std::to_string(name_no++);
    }

    Aggregator::Params getAggregateParam(const Block & header, const ColumnNumbers & keys, const AggregateDescriptions & aggregates)
    {
        Settings settings;
        return Aggregator::Params(
            header,
            keys,
            aggregates,
            false,
            settings.max_rows_to_group_by,
            settings.group_by_overflow_mode,
            settings.group_by_two_level_threshold,
            settings.group_by_two_level_threshold_bytes,
            settings.max_bytes_before_external_group_by,
            settings.empty_result_for_aggregation_by_empty_set,
            nullptr,
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            true,
            3);
    }

    Aggregator::Params getMergedAggregateParam(const Block & header, const ColumnNumbers & keys, const AggregateDescriptions & aggregates)
    {
        Settings settings;
        return Aggregator::Params(header, keys, aggregates, false, settings.max_threads);
    }


    int name_no = 0;
    std::unordered_map<std::string, std::string> function_mapping;
    std::vector<jobject> input_iters;
    ContextPtr context;
    JavaVM* vm;


//    DB::QueryPlanPtr query_plan;

};

struct SparkBuffer
{
    uint8_t * address;
    size_t size;
};

class LocalExecutor
{
public:
    LocalExecutor() = default;
    explicit LocalExecutor(QueryContext& _query_context);
    void execute(QueryPlanPtr query_plan);
    SparkRowInfoPtr next();
    Block* nextColumnar();
    bool hasNext();
    ~LocalExecutor()
    {
        if (this->spark_buffer)
        {
            this->ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
            this->spark_buffer.reset();
        }
    }

    Block & getHeader();

private:
    QueryContext query_context;
    std::unique_ptr<SparkRowInfo> writeBlockToSparkRow(DB::Block & block);
    QueryPipeline query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block header;
    std::unique_ptr<CHColumnToSparkRow> ch_column_to_spark_row;
    std::unique_ptr<Block> current_chunk;
    std::unique_ptr<SparkBuffer> spark_buffer;
};
}
