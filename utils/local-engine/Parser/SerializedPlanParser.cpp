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
        return factory.get("UInt8");
    }
    else if (type.has_i16())
    {
        return factory.get("UInt16");
    }
    else if (type.has_i32())
    {
        return factory.get("UInt32");
    }
    else if (type.has_string())
    {
        return factory.get("String");
    }
    else
    {
        throw std::runtime_error("doesn't support type " + type.DebugString());
    }
}
DB::QueryPlanPtr dbms::SerializedPlanParser::parse(std::unique_ptr<io::substrait::Plan> plan)
{
    auto query_plan = std::make_unique<DB::QueryPlan>();
    if (plan->relations().Capacity() == 1)
    {
        auto rel = plan->relations().at(0);
        if (rel.has_read()) {
            std::shared_ptr<IProcessor> source = std::dynamic_pointer_cast<IProcessor>(SerializedPlanParser::parseReadRealWithLocalFile(rel.read()));
            auto source_step = std::make_unique<ReadFromStorageStep>(Pipe(source), "Parquet");
            query_plan->addStep(std::move(source_step));
        }
        else
        {
            throw std::runtime_error("unsupported relation");
        }
    }
    else
    {
        throw std::runtime_error("unsupported relation");
    }
    return query_plan;
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
    auto query_pipeline = query_plan->buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings());
    this->executor = std::make_unique<DB::PullingPipelineExecutor>(*query_pipeline);
    this->header = query_plan->getCurrentDataStream().header;
    this->ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(header, "Arrow", false);
}
void dbms::LocalExecutor::writeChunkToArrowString(DB::Chunk &chunk, std::string & arrowChunk)
{
    std::shared_ptr<arrow::Table> arrow_table;
    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunk, chunk.getNumColumns());
    DB::WriteBufferFromString buf(arrowChunk);
    auto out_stream = std::make_shared<ArrowBufferedOutputStream>(buf);
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> writer_status;
    writer_status = arrow::ipc::MakeFileWriter(out_stream.get(), arrow_table->schema());
    if (!writer_status.ok())
        throw std::runtime_error("Error while opening a table writer");
    auto writer = *writer_status;
    auto write_status = writer->WriteTable(*arrow_table, 1000000);
    if (writer_status.ok())
    {
        throw std::runtime_error("Error while writing a table");
    }
    auto close_status = writer->Close();
    if (close_status.ok())
    {
        throw std::runtime_error("Error while close a table");
    }
}
