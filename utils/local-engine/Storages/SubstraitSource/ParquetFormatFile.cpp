#include <memory>
#include <string>
#include <utility>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/ArrowParquetBlockInputFormat.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
ParquetFormatFile::ParquetFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(const DB::Block & header)
{
    auto row_group_indices = collectRowGroupIndices();
    auto read_buffer = read_buffer_builder->build(file_info);
    auto input_format = std::make_shared<local_engine::ArrowParquetBlockInputFormat>(*read_buffer, header, DB::FormatSettings(), row_group_indices);
    auto res = std::make_shared<FormatFile::InputFormat>(input_format, std::move(read_buffer));
    return res;
}

std::optional<size_t> ParquetFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }
    auto row_group_indices = collectRowGroupIndices();
    size_t rows = 0;
    auto file_meta = reader->parquet_reader()->metadata();
    for (auto i : row_group_indices)
    {
        rows += file_meta->RowGroup(i)->num_rows();
    }
    {
        std::lock_guard lock(mutex);
        total_rows = rows;
        return total_rows;
    }
}

void ParquetFormatFile::prepareReader()
{
    std::lock_guard lock(mutex);
    if (reader)
        return;
    auto in = read_buffer_builder->build(file_info);
    DB::FormatSettings format_settings;
    format_settings.seekable_read = true;
    std::atomic<int> is_stopped{0};
    auto status = parquet::arrow::OpenFile(
        asArrowFile(*in, format_settings, is_stopped), arrow::default_memory_pool(), &reader);
    if (!status.ok())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Open file({}) failed. {}", file_info.uri_file(), status.ToString());
    }
}

std::vector<int> ParquetFormatFile::collectRowGroupIndices()
{
    prepareReader();
    auto file_meta = reader->parquet_reader()->metadata();
    std::vector<int> indices;
    for (int i = 0, n = file_meta->num_row_groups(); i < n; ++i)
    {
        auto row_group_meta = file_meta->RowGroup(i);
        auto offset = static_cast<UInt64>(row_group_meta->file_offset());
        if (!offset)
        {
            offset = static_cast<UInt64>(row_group_meta->ColumnChunk(0)->file_offset());
        }
        if (file_info.start() <=  offset && offset < file_info.start() + file_info.length())
        {
            indices.push_back(i);
        }
    }
    return indices;
}
}
