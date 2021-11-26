#pragma once

#include <Substrait/plan.pb.h>
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


class SerializedPlanParser
{
public:
    static DB::QueryPlanPtr parse(std::string& plan);
    static DB::QueryPlanPtr parse(std::unique_ptr<io::substrait::Plan> plan);
    static DB::BatchParquetFileSourcePtr parseReadRealWithLocalFile(const io::substrait::ReadRel& rel);
    static DB::Block parseNameStruct(const io::substrait::Type_NamedStruct& struct_);
    static DB::DataTypePtr parseType(const io::substrait::Type& type);
};


class LocalExecutor
{
public:
    void execute(QueryPlanPtr query_plan);
    std::string next();
    bool hasNext();

private:
    void writeChunkToArrowString(Chunk& chunk, std::string & arrow_chunk);
    QueryPipelinePtr query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block header;
    std::unique_ptr<DB::CHColumnToArrowColumn> ch_column_to_arrow_column;
    std::unique_ptr<Chunk> current_chunk;
};
}


