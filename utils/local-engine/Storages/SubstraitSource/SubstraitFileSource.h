#pragma once
#include <cstdint>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <base/types.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
namespace local_engine
{

class FileReaderWrapper
{
public:
    explicit FileReaderWrapper(FormatFilePtr file_) : file(file_) {}
    virtual ~FileReaderWrapper() = default;
    virtual bool pull(DB::Chunk & chunk) = 0;

protected:
    FormatFilePtr file;

    static DB::ColumnPtr createConstColumn(DB::DataTypePtr type, const DB::Field & field, size_t rows);
    static DB::Field buildFieldFromString(const String & value, DB::DataTypePtr type);
};

class NormalFileReader : public FileReaderWrapper
{
public:
    NormalFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & to_read_header_, const DB::Block & output_header_);
    ~NormalFileReader() override = default;
    bool pull(DB::Chunk & chunk) override;

private:
    DB::ContextPtr context;
    DB::Block to_read_header;
    DB::Block output_header;

    FormatFile::InputFormatPtr input_format;
    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PullingPipelineExecutor> reader;
};

class EmptyFileReader : public FileReaderWrapper
{
public:
    explicit EmptyFileReader(FormatFilePtr file_) : FileReaderWrapper(file_) {}
    ~EmptyFileReader() override = default;
    bool pull(DB::Chunk &) override { return false; }
};

class ConstColumnsFileReader : public FileReaderWrapper
{
public:
    ConstColumnsFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & header_, size_t block_size_ = DEFAULT_BLOCK_SIZE);
    ~ConstColumnsFileReader() override = default;
    bool pull(DB::Chunk & chunk);
private:
    DB::ContextPtr context;
    DB::Block header;
    size_t remained_rows;
    size_t block_size;
};

class SubstraitFileSource : public DB::SourceWithProgress
{
public:
    SubstraitFileSource(DB::ContextPtr context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos);
    ~SubstraitFileSource() override = default;

    String getName() const override
    {
        return "SubstraitFileSource";
    }
protected:
    DB::Chunk generate() override;
private:
    DB::ContextPtr context;
    DB::Block output_header;
    DB::Block to_read_header;
    FormatFiles files;

    UInt32 current_file_index = 0;
    std::unique_ptr<FileReaderWrapper> file_reader;
    ReadBufferBuilderPtr read_buffer_builder;

    bool tryPrepareReader();
};
}
