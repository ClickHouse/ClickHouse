#include "SerializedPlanParser.h"
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowBlockOutputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Pipe.h>
#include <IO/WriteBufferFromString.h>
#include <sys/stat.h>

DB::BatchParquetFileSourcePtr dbms::SerializedPlanParser::parseReadRealWithLocalFile(const io::substrait::ReadRel& rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    auto files_info = std::make_shared<FilesInfo>();
    for (const auto &item : rel.local_files().items())
    {
        files_info->files.push_back(item.uri_path());
    }
    return std::make_shared<BatchParquetFileSource>(files_info, parseNameStruct(rel.base_schema()));
}

DB::Block dbms::SerializedPlanParser::parseNameStruct(const io::substrait::Type_NamedStruct & struct_)
{
    auto internal_cols = std::make_unique<std::vector<DB::ColumnWithTypeAndName>>();
    internal_cols->reserve(struct_.names_size());
    for (int i = 0; i < struct_.names_size(); ++i)
    {
        const auto& name = struct_.names(i);
        const auto& type = struct_.struct_().types(i);
        auto data_type = parseType(type);
        internal_cols->push_back(DB::ColumnWithTypeAndName(data_type->createColumn(), data_type, name));
    }
    return DB::Block(*std::move(internal_cols));
}
DB::DataTypePtr dbms::SerializedPlanParser::parseType(const io::substrait::Type& type)
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
    else
    {
        throw std::runtime_error("doesn't support type " + type.DebugString());
    }
}
DB::QueryPlanPtr dbms::SerializedPlanParser::parse(std::unique_ptr<io::substrait::Plan> plan)
{
    auto query_plan = std::make_unique<DB::QueryPlan>();
    if (plan->relations_size() == 1)
    {
        auto rel = plan->relations().at(0);
        parse(*query_plan, rel);
    }
    else
    {
        throw std::runtime_error("too many relations found");
    }
    return query_plan;
}
DB::QueryPlanPtr dbms::SerializedPlanParser::parse(std::string& plan)
{
    auto plan_ptr = std::make_unique<io::substrait::Plan>();
    plan_ptr->ParseFromString(plan);
    return parse(std::move(plan_ptr));
}
void dbms::SerializedPlanParser::parse(DB::QueryPlan & query_plan, const io::substrait::ReadRel & rel)
{
    std::shared_ptr<IProcessor> source = std::dynamic_pointer_cast<IProcessor>(SerializedPlanParser::parseReadRealWithLocalFile(rel));
    auto source_step = std::make_unique<ReadFromStorageStep>(Pipe(source), "Parquet");
    query_plan.addStep(std::move(source_step));
}
void dbms::SerializedPlanParser::parse(DB::QueryPlan & query_plan, const io::substrait::Rel& rel)
{
    if (rel.has_read()) {
        parse(query_plan, rel.read());
    }
    else if (rel.has_project())
    {
        parse(query_plan, rel.project());
    }
    else
    {
        throw std::runtime_error("unsupported relation");
    }
}
void dbms::SerializedPlanParser::parse(DB::QueryPlan & query_plan, const io::substrait::ProjectRel & rel)
{
    if (rel.has_input())
    {
        parse(query_plan, rel.input());
    }
    else
    {
        throw std::runtime_error("project relation should contains a input relation");
    }
    //TODO add project step
}
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

            struct stat file_stat{};

            /// Check if file descriptor allows random reads (and reading it twice).
            if (0 != stat(current_path.c_str(), &file_stat))
                throw std::runtime_error("Cannot stat file " + current_path);

            if (S_ISREG(file_stat.st_mode))
                nested_buffer = std::make_unique<ReadBufferFromFilePRead>(current_path);
            else
                nested_buffer = std::make_unique<ReadBufferFromFile>(current_path);


            read_buf = std::move(nested_buffer);
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
DB::BatchParquetFileSource::BatchParquetFileSource(
    FilesInfoPtr files, const DB::Block & sample)
    : SourceWithProgress(sample), files_info(files), header(sample)
{
}
void dbms::LocalExecutor::execute(DB::QueryPlanPtr query_plan)
{
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
    this->query_pipeline = query_plan->buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings());
    this->executor = std::make_unique<DB::PullingPipelineExecutor>(*query_pipeline);
    this->header = query_plan->getCurrentDataStream().header;
    this->ch_column_to_spark_row = std::make_unique<local_engine::CHColumnToSparkRow>();
}
std::unique_ptr<local_engine::SparkRowInfo> dbms::LocalExecutor::writeBlockToSparkRow(DB::Block &block)
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
    } else {
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
