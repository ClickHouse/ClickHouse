#include "ArrowParquetBlockInputFormat.h"

#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <boost/range/irange.hpp>


namespace local_engine
{
ArrowParquetBlockInputFormat::ArrowParquetBlockInputFormat(
    DB::ReadBuffer & in_, const DB::Block & header, const DB::FormatSettings & formatSettings, size_t prefer_block_size_)
    : ParquetBlockInputFormat(in_, header, formatSettings), prefer_block_size(prefer_block_size_)
{
}

void ArrowParquetBlockInputFormat::prepareRecordBatchReader(const arrow::Table & table)
{
    auto row_groups = boost::irange(0, file_reader->num_row_groups());
    auto row_group_vector = std::vector<int>(row_groups.begin(), row_groups.end());
    auto reader = std::make_shared<arrow::TableBatchReader>(table);
    reader->set_chunksize(1024);
    current_record_batch_reader = reader;
}

DB::Chunk ArrowParquetBlockInputFormat::generate()
{
    DB::Chunk res;
    block_missing_values.clear();

    if (!file_reader)
    {
        prepareReader();
    }

    if (is_stopped)
        return {};

    if (!current_row_group_table)
    {
        arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &current_row_group_table);
        if (!read_status.ok())
            throw std::runtime_error{"Error while reading Parquet data: " + read_status.ToString()};
        if (format_settings.use_lowercase_column_name)
            current_row_group_table = *current_row_group_table->RenameColumns(column_names);
        prepareRecordBatchReader(*current_row_group_table);
        ++row_group_current;
    }


    while (buffer.size() < prefer_block_size)
    {
        DB::Chunk chunk;
        auto batch = current_record_batch_reader->Next();
        if (!*batch)
        {
            // all row group end
            if (row_group_current >= row_group_total)
            {
                break;
            }
            else
            {
                // init next row group table reader
                arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &current_row_group_table);
                if (!read_status.ok())
                    throw std::runtime_error{
                        "Error while reading Parquet data: " + read_status.ToString()};
                if (format_settings.use_lowercase_column_name)
                    current_row_group_table = *current_row_group_table->RenameColumns(column_names);
                prepareRecordBatchReader(*current_row_group_table);
                ++row_group_current;
            }
        }
        else
        {
            auto tmp_table = arrow::Table::FromRecordBatches({*batch});
            arrow_column_to_ch_column->arrowTableToCHChunk(chunk, *tmp_table);
            buffer.add(chunk, 0, chunk.getNumRows());
        }
    }

    res = buffer.releaseColumns();
    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    if (format_settings.defaults_for_omitted_fields)
        for (size_t row_idx = 0; row_idx < res.getNumRows(); ++row_idx)
            for (const auto & column_idx : missing_columns)
                block_missing_values.setBit(column_idx, row_idx);
    return res;
}

}
