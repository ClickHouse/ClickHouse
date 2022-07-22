#include "ArrowParquetBlockInputFormat.h"

#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <arrow/record_batch.h>
#include <Common/Stopwatch.h>
#include <arrow/table.h>
#include <boost/range/irange.hpp>
#include <DataTypes/NestedUtils.h>

using namespace DB;

namespace local_engine
{
ArrowParquetBlockInputFormat::ArrowParquetBlockInputFormat(
    DB::ReadBuffer & in_, const DB::Block & header, const DB::FormatSettings & formatSettings)
    : ParquetBlockInputFormat(in_, header, formatSettings)
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
        std::shared_ptr<::arrow::Schema> schema;
        file_reader->GetSchema(&schema);
//        file_reader->set_use_threads(true);
        std::unordered_set<String> nested_table_names;
        if (format_settings.parquet.import_nested)
            nested_table_names = Nested::getAllTableNames(getPort().getHeader());
        int index = 0;
        for (int i = 0; i < schema->num_fields(); ++i)
        {
            /// STRUCT type require the number of indexes equal to the number of
            /// nested elements, so we should recursively
            /// count the number of indices we need for this type.
            int indexes_count = countIndicesForType(schema->field(i)->type());
            const auto & name = schema->field(i)->name();
            if (getPort().getHeader().has(name) || nested_table_names.contains(name))
            {
                for (int j = 0; j != indexes_count; ++j)
                {
                    column_indices.push_back(index + j);
                    column_names.push_back(name);
                }
            }
            index += indexes_count;
        }
        auto row_group_range = boost::irange(0, file_reader->num_row_groups());
        auto row_group_indices = std::vector(row_group_range.begin(), row_group_range.end());
        auto read_status = file_reader->GetRecordBatchReader(row_group_indices, column_indices, &current_record_batch_reader);
        if (!read_status.ok())
            throw std::runtime_error{"Error while reading Parquet data: " + read_status.ToString()};
    }

    if (is_stopped)
        return {};


    auto batch = current_record_batch_reader->Next();
    if (*batch)
    {
        auto tmp_table = arrow::Table::FromRecordBatches({*batch});
        arrow_column_to_ch_column->arrowTableToCHChunk(res, *tmp_table);
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
