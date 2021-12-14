#include "SparkColumnToCHColumn.h"
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>

using namespace DB;

namespace local_engine
{
int64_t getStringColumnTotalSize(int ordinal, SparkRowInfo & spark_row_info)
{
    SparkRowReader reader(spark_row_info.getNumCols());
    int64_t size = 0;
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        reader.pointTo(reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        size += reader.getStringSize(ordinal);
    }
    return size;
}

static void writeRowToColumns(ColumnsWithTypeAndName & columns, SparkRowReader & spark_row_reader, SparkRowInfo & spark_row_info)
{
    int32_t num_fields = columns.size();
    for (int32_t i = 0; i < num_fields; i++)
    {
        auto column = columns[i];
        WhichDataType which(column.type);
        if (which.isUInt8())
        {
            auto & column_data = assert_cast<ColumnVector<UInt8> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getUnsignedByte(i));
        }
        else if (which.isInt8())
        {
            auto & column_data = assert_cast<ColumnVector<Int8> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getByte(i));
        }
        else if (which.isInt16())
        {
            auto & column_data = assert_cast<ColumnVector<Int16> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getShort(i));        }
        else if (which.isInt32())
        {
            auto & column_data = assert_cast<ColumnVector<Int32> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getInt(i));
        }
        else if (which.isInt64())
        {
            auto & column_data = assert_cast<ColumnVector<Int64> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getLong(i));
        }
        else if (which.isFloat32())
        {
            auto & column_data = assert_cast<ColumnVector<Float32> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getFloat(i));
        }
        else if (which.isFloat64())
        {
            auto & column_data = assert_cast<ColumnVector<Float64> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getDouble(i));
        }
        else if (which.isDate())
        {
            auto & column_data = assert_cast<ColumnVector<UInt16> &>(column.column).getData();
            column_data.emplace_back(spark_row_reader.getUnsignedShort(i));
        }
        else if (which.isString())
        {
            PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(column.column).getChars();
            PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(column.column).getOffsets();
            if (static_cast<int64_t>(column_offsets.capacity()) < spark_row_info.getNumRows())
            {
                auto total_size = getStringColumnTotalSize(i, spark_row_info);
                column_chars_t.reserve(total_size);
                column_offsets.reserve(spark_row_info.getNumRows());
            }
            std::string data = spark_row_reader.getString(i);
            column_chars_t.insert_assume_reserved(data.data(), data.size());
            column_chars_t.emplace_back('\0');
            column_offsets.emplace_back(column_chars_t.size());
        }
        else
        {
            throw std::runtime_error("doesn't support type " + std::string(getTypeName(column.type->getTypeId())));
        }
    }
}

std::unique_ptr<Block>
local_engine::SparkColumnToCHColumn::convertCHColumnToSparkRow(local_engine::SparkRowInfo & spark_row_info, DB::Block& header)
{
    auto columns_list = std::make_unique<ColumnsWithTypeAndName>();
    columns_list->reserve(header.columns());
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        MutableColumnPtr read_column = header_column.type->createColumn();
        read_column->reserve(spark_row_info.getNumRows());
        ColumnWithTypeAndName column;
        column.name = header_column.name;
        column.type = header_column.type;
        column.column = std::move(read_column);
        columns_list->push_back(std::move(column));
    }
    SparkRowReader row_reader(header.columns());
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        row_reader.pointTo(
            reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        writeRowToColumns(*columns_list, row_reader, spark_row_info);
    }
    return std::make_unique<Block>(*std::move(columns_list));
}



}
