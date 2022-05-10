#pragma once

#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/ChunkBuffer.h>
#include <parquet/arrow/reader.h>

namespace arrow {
class RecordBatchReader;
class Table;
}

namespace local_engine
{
class ArrowParquetBlockInputFormat : public DB::ParquetBlockInputFormat
{
public:
    ArrowParquetBlockInputFormat(
        DB::ReadBuffer & in, const DB::Block & header, const DB::FormatSettings & formatSettings, size_t prefer_block_size_  = 8192);

private:
    void prepareRecordBatchReader(const arrow::Table & table);
    DB::Chunk generate() override;

    size_t prefer_block_size;
    std::shared_ptr<arrow::RecordBatchReader> current_record_batch_reader;
    std::shared_ptr<arrow::Table> current_row_group_table;
    local_engine::ChunkBuffer buffer;

};

}
