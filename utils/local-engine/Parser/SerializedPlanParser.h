#pragma once

#include <substrait/plan.pb.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/IStorage.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <arrow/ipc/writer.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Storages/CustomStorageMergeTree.h>
#include <local/LocalServer.h>
#include "CHColumnToSparkRow.h"

namespace DB
{

struct FilesInfo
{
    std::vector<std::string> files;
    std::atomic<size_t> next_file_to_read = 0;
};
using FilesInfoPtr = std::shared_ptr<FilesInfo>;

class BatchParquetFileSource : public DB::SourceWithProgress
{
public:
    BatchParquetFileSource(FilesInfoPtr files, const Block & header);

private:
    String getName() const override
    {
        return "BatchParquetFileSource";
    }

protected:
    Chunk generate() override;

private:
    FilesInfoPtr files_info;
    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    bool finished_generate = false;
    std::string current_path;
    Block header;
};

using BatchParquetFileSourcePtr = std::shared_ptr<BatchParquetFileSource>;
}


namespace dbms
{
using namespace DB;


static const std::map<std::string, std::string> SCALAR_FUNCTIONS = {
    {"IS_NOT_NULL","isNotNull"},
    {"GREATER_THAN_OR_EQUAL","greaterOrEquals"},
    {"AND", "and"},
    {"LESS_THAN_OR_EQUAL", "lessOrEquals"},
    {"LESS_THAN", "less"},
    {"MULTIPLY", "multiply"},
    {"SUM", "sum"},
    {"TO_DATE", "toDate"},
    {"EQUAL_TO", "equals"}
};

struct QueryContext {
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata;
    std::shared_ptr<local_engine::CustomStorageMergeTree> custom_storage_merge_tree;
};

class SerializedPlanParser
{
public:
    SerializedPlanParser(const ContextPtr & context);
    static void initFunctionEnv();
    DB::QueryPlanPtr parse(std::string& plan);
    DB::QueryPlanPtr parse(std::unique_ptr<substrait::Plan> plan);

    DB::BatchParquetFileSourcePtr parseReadRealWithLocalFile(const substrait::ReadRel& rel);
    DB::QueryPlanPtr parseMergeTreeTable(const substrait::ReadRel& rel);
    DB::Block parseNameStruct(const substrait::NamedStruct& struct_);
    DB::DataTypePtr parseType(const substrait::Type& type);

    static ContextMutablePtr global_context;
    static std::unique_ptr<DB::LocalServer> local_server;
    static SharedContextHolder shared_context;
    QueryContext query_context;

private:
    static DB::NamesAndTypesList blockToNameAndTypeList(const DB::Block & header);
    DB::QueryPlanPtr parseOp(const substrait::Rel &rel);
    DB::ActionsDAGPtr parseFunction(const DataStream & input, const substrait::Expression &rel, std::string & result_name, DB::ActionsDAGPtr actions_dag = nullptr, bool keep_result = false);
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
            settings.compile_aggregate_expressions,
            settings.min_count_to_compile_aggregate_expression);
    }


    int name_no = 0;
    std::unordered_map<std::string, std::string> function_mapping;
    ContextPtr context;


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

    LocalExecutor();

    explicit LocalExecutor(QueryContext& _query_context);
    void execute(QueryPlanPtr query_plan);
    local_engine::SparkRowInfoPtr next();
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
    std::unique_ptr<local_engine::SparkRowInfo> writeBlockToSparkRow(DB::Block & block);
    QueryPipelinePtr query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block header;
    std::unique_ptr<local_engine::CHColumnToSparkRow> ch_column_to_spark_row;
    std::unique_ptr<Block> current_chunk;
    std::unique_ptr<SparkBuffer> spark_buffer;
};
}


