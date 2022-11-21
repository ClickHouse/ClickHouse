#include "ArrowParquetBlockInputFormat.h"

#include <arrow/record_batch.h>
#include <Common/Stopwatch.h>
#include <arrow/table.h>
#include <boost/range/irange.hpp>
#include <DataTypes/NestedUtils.h>

#include "ch_parquet/OptimizedArrowColumnToCHColumn.h"

using namespace DB;

namespace local_engine
{
ArrowParquetBlockInputFormat::ArrowParquetBlockInputFormat(
    DB::ReadBuffer & in_, const DB::Block & header, const DB::FormatSettings & formatSettings, const std::vector<int> & row_group_indices_)
    : OptimizedParquetBlockInputFormat(in_, header, formatSettings)
    , row_group_indices(row_group_indices_)
{
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 0;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type());
    }

    return 1;
}

DB::Chunk ArrowParquetBlockInputFormat::generate()
{
    DB::Chunk res;
    block_missing_values.clear();

    if (!file_reader)
    {
        prepareReader();
        file_reader->set_batch_size(8192);
        if (row_group_indices.empty())
        {
            auto row_group_range = boost::irange(0, file_reader->num_row_groups());
            row_group_indices = std::vector(row_group_range.begin(), row_group_range.end());
        }
        auto read_status = file_reader->GetRecordBatchReader(row_group_indices, column_indices, &current_record_batch_reader);
        if (!read_status.ok())
            throw std::runtime_error{"Error while reading Parquet data: " + read_status.ToString()};
    }

    if (is_stopped)
        return {};


    Stopwatch watch;
    watch.start();
    auto batch = current_record_batch_reader->Next();
    if (*batch)
    {
        auto tmp_table = arrow::Table::FromRecordBatches({*batch});
        if (format_settings.use_lowercase_column_name)
        {
            tmp_table = (*tmp_table)->RenameColumns(column_names);
        }
        non_convert_time += watch.elapsedNanoseconds();
        watch.restart();
        arrow_column_to_ch_column->arrowTableToCHChunk(res, *tmp_table);
        convert_time += watch.elapsedNanoseconds();
    }
    else
    {
        current_record_batch_reader.reset();
        file_reader.reset();
        return {};
    }

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    if (format_settings.defaults_for_omitted_fields)
        for (size_t row_idx = 0; row_idx < res.getNumRows(); ++row_idx)
            for (const auto & column_idx : missing_columns)
                block_missing_values.setBit(column_idx, row_idx);
    return res;
}

}
