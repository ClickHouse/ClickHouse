#include "SparkRowToCHColumn.h"
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

using namespace DB;

namespace local_engine
{
jclass SparkRowToCHColumn::spark_row_interator_class = nullptr;
jmethodID SparkRowToCHColumn::spark_row_interator_hasNext = nullptr;
jmethodID SparkRowToCHColumn::spark_row_interator_next = nullptr;
jmethodID SparkRowToCHColumn::spark_row_iterator_nextBatch = nullptr;

int64_t getStringColumnTotalSize(int ordinal, SparkRowInfo & spark_row_info)
{
    SparkRowReader reader(spark_row_info.getNumCols());
    int64_t size = 0;
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        reader.pointTo(
            reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        size += (reader.getStringSize(ordinal) + 1);
    }
    return size;
}

void writeRowToColumns(std::vector<MutableColumnPtr> & columns,std::vector<DataTypePtr>& types, SparkRowReader & spark_row_reader)
{
    int32_t num_fields = columns.size();
    [[maybe_unused]] bool is_nullable = false;
    for (int32_t i = 0; i < num_fields; i++)
    {
        WhichDataType which(columns[i]->getDataType());
        if (which.isNullable())
        {
            const auto * nullable = checkAndGetDataType<DataTypeNullable>(types[i].get());
            which = WhichDataType(nullable->getNestedType());
            is_nullable = true;
        }

        if (spark_row_reader.isNullAt(i)) {
            assert(is_nullable);
            ColumnNullable & column = assert_cast<ColumnNullable &>(*columns[i]);
            column.insertData(nullptr, 0);
            continue;
        }

        if (which.isUInt8())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(uint8_t));
        }
        else if (which.isInt8())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int8_t));
        }
        else if (which.isInt16())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int16_t));
        }
        else if (which.isInt32())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int32_t));
        }
        else if (which.isInt64() || which.isDateTime64())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int64_t));
        }
        else if (which.isFloat32())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(float_t));
        }
        else if (which.isFloat64())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(double_t));
        }
        else if (which.isDate() || which.isUInt16())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(uint16_t));
        }
        else if (which.isDate32() || which.isUInt32())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(uint32_t));
        }
        else if (which.isString())
        {
            StringRef data = spark_row_reader.getString(i);
            columns[i]->insertData(data.data, data.size);
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support type {} convert from spark row to ch columnar" ,
                            magic_enum::enum_name(columns[i]->getDataType()));
        }
    }
}

std::unique_ptr<Block>
local_engine::SparkRowToCHColumn::convertSparkRowInfoToCHColumn(local_engine::SparkRowInfo & spark_row_info, DB::Block & header)
{
    auto columns_list = std::make_unique<ColumnsWithTypeAndName>();
    columns_list->reserve(header.columns());
    std::vector<MutableColumnPtr> mutable_columns;
    std::vector<DataTypePtr> types;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        MutableColumnPtr read_column = header_column.type->createColumn();
        read_column->reserve(spark_row_info.getNumRows());
        mutable_columns.push_back(std::move(read_column));
        types.push_back(header_column.type);
    }
    SparkRowReader row_reader(header.columns());
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        row_reader.pointTo(
            reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        writeRowToColumns(mutable_columns, types, row_reader);
    }
    auto block = std::make_unique<Block>(*std::move(columns_list));
    for (size_t column_i = 0, columns = mutable_columns.size(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        ColumnWithTypeAndName column(std::move(mutable_columns[column_i]), header_column.type, header_column.name);
        block->insert(column);
    }
    mutable_columns.clear();
    return block;
}

void local_engine::SparkRowToCHColumn::appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, int64_t address, int32_t size)
{
    SparkRowReader row_reader(helper.header->columns());
    row_reader.pointTo(address, size);
    writeRowToColumns(*helper.cols, *helper.typePtrs, row_reader);
}

Block * local_engine::SparkRowToCHColumn::getWrittenBlock(SparkRowToCHColumnHelper & helper)
{
    auto * block = new Block();
    for (size_t column_i = 0, columns = helper.cols->size(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = helper.header->getByPosition(column_i);
        ColumnWithTypeAndName column(std::move(helper.cols->operator[](column_i)), header_column.type, header_column.name);
        block->insert(column);
    }
    return block;
}

}
