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
        size += (reader.getStringSize(ordinal) + 1);
    }
    return size;
}

static void writeRowToColumns(std::vector<MutableColumnPtr> & columns, SparkRowReader & spark_row_reader, SparkRowInfo & spark_row_info, int64_t row_num)
{
    int32_t num_fields = columns.size();
    for (int32_t i = 0; i < num_fields; i++)
    {
//        auto column = columns[i];
        WhichDataType which(columns[i]->getDataType());
        if (which.isUInt8())
        {
            auto & column_data = assert_cast<ColumnVector<UInt8> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getUnsignedByte(i));
        }
        else if (which.isInt8())
        {
            auto & column_data = assert_cast<ColumnVector<Int8> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getByte(i));
        }
        else if (which.isInt16())
        {
            auto & column_data = assert_cast<ColumnVector<Int16> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getShort(i));        }
        else if (which.isInt32())
        {
            auto & column_data = assert_cast<ColumnVector<Int32> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getInt(i));
        }
        else if (which.isInt64())
        {
            auto & column_data = assert_cast<ColumnVector<Int64> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getLong(i));
        }
        else if (which.isFloat32())
        {
            auto & column_data = assert_cast<ColumnVector<Float32> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getFloat(i));
        }
        else if (which.isFloat64())
        {
            auto & column_data = assert_cast<ColumnVector<Float64> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getDouble(i));
        }
        else if (which.isDate() || which.isUInt16())
        {
            auto & column_data = assert_cast<ColumnVector<UInt16> &>(*columns[i]).getData();
            column_data.emplace_back(spark_row_reader.getUnsignedShort(i));
        }
        else if (which.isString())
        {
            PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*columns[i]).getChars();
            PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*columns[i]).getOffsets();
//            auto capacity column_offsets.capacity();
            if (row_num == 0)
            {
                auto total_size = getStringColumnTotalSize(i, spark_row_info);
                column_chars_t.reserve(total_size);
                column_offsets.reserve(spark_row_info.getNumRows());
            }
            StringRef data = spark_row_reader.getString(i);
            column_chars_t.insert_assume_reserved(data.data, data.data+data.size);
            column_chars_t.emplace_back('\0');
            column_offsets.emplace_back(column_chars_t.size());
        }
        else
        {
            throw std::runtime_error("doesn't support type " + std::string(getTypeName(columns[i]->getDataType())));
        }
    }
}

std::unique_ptr<Block>
local_engine::SparkColumnToCHColumn::convertCHColumnToSparkRow(local_engine::SparkRowInfo & spark_row_info, DB::Block& header)
{
    auto columns_list = std::make_unique<ColumnsWithTypeAndName>();
    columns_list->reserve(header.columns());
    std::vector<MutableColumnPtr> mutable_columns;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        MutableColumnPtr read_column = header_column.type->createColumn();
        read_column->reserve(spark_row_info.getNumRows());
        mutable_columns.push_back(std::move(read_column));
    }
    SparkRowReader row_reader(header.columns());
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        row_reader.pointTo(
            reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        writeRowToColumns(mutable_columns, row_reader, spark_row_info, i);
    }
    auto block = std::make_unique<Block>(*std::move(columns_list));
    for (size_t column_i = 0, columns = mutable_columns.size(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        ColumnWithTypeAndName column(std::move(mutable_columns[column_i]), header_column.type,header_column.name);
        block->insert(column);
    }
    mutable_columns.clear();
    return block;
}



}
