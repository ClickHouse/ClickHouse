#include "PartitionColumnFillingTransform.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/StringUtils.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
template <typename Type>
requires(
    std::is_same_v<Type, Int8> || std::is_same_v<Type, UInt16> || std::is_same_v<Type, Int16> || std::is_same_v<Type, Int32> || std::is_same_v<Type, Int64>)
    ColumnPtr createIntPartitionColumn(DataTypePtr column_type, std::string partition_value)
{
    Type value;
    auto value_buffer = ReadBufferFromString(partition_value);
    readIntText(value, value_buffer);
    return column_type->createColumnConst(1, value);
}

template <typename Type>
requires(std::is_same_v<Type, Float32> || std::is_same_v<Type, Float64>) ColumnPtr
    createFloatPartitionColumn(DataTypePtr column_type, std::string partition_value)
{
    Type value;
    auto value_buffer = ReadBufferFromString(partition_value);
    readFloatText(value, value_buffer);
    return column_type->createColumnConst(1, value);
}

//template <>
//ColumnPtr createFloatPartitionColumn<Float32>(DataTypePtr column_type, std::string partition_value);
//template <>
//ColumnPtr createFloatPartitionColumn<Float64>(DataTypePtr column_type, std::string partition_value);

PartitionColumnFillingTransform::PartitionColumnFillingTransform(
    const DB::Block & input_, const DB::Block & output_, const String & partition_col_name_, const String & partition_col_value_)
    : ISimpleTransform(input_, output_, true), partition_col_name(partition_col_name_), partition_col_value(partition_col_value_)
{
    partition_col_type = output_.getByName(partition_col_name_).type;
    partition_column = createPartitionColumn();
}

ColumnPtr PartitionColumnFillingTransform::createPartitionColumn()
{
    ColumnPtr result;
    DataTypePtr nested_type = partition_col_type;
    if (const DataTypeNullable * nullable_type = checkAndGetDataType<DataTypeNullable>(partition_col_type.get()))
    {
        nested_type = nullable_type->getNestedType();
        if (StringUtils::isNullPartitionValue(partition_col_value))
        {
            return nullable_type->createColumnConstWithDefaultValue(1);
        }
    }
    WhichDataType which(nested_type);
    if (which.isInt8())
    {
        result = createIntPartitionColumn<Int8>(partition_col_type, partition_col_value);
    }
    else if (which.isInt16())
    {
        result = createIntPartitionColumn<Int16>(partition_col_type, partition_col_value);
    }
    else if (which.isInt32())
    {
        result = createIntPartitionColumn<Int32>(partition_col_type, partition_col_value);
    }
    else if (which.isInt64())
    {
        result = createIntPartitionColumn<Int64>(partition_col_type, partition_col_value);
    }
    else if (which.isFloat32())
    {
        result = createFloatPartitionColumn<Float32>(partition_col_type, partition_col_value);
    }
    else if (which.isFloat64())
    {
        result = createFloatPartitionColumn<Float64>(partition_col_type, partition_col_value);
    }
    else if (which.isDate())
    {
        DayNum value;
        auto value_buffer = ReadBufferFromString(partition_col_value);
        readDateText(value, value_buffer);
        result = partition_col_type->createColumnConst(1, value);
    }
    else if (which.isString())
    {
        result = partition_col_type->createColumnConst(1, partition_col_value);
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported datatype {}", partition_col_type->getFamilyName());
    }
    return result;
}

void PartitionColumnFillingTransform::transform(DB::Chunk & chunk)
{
    size_t partition_column_position = output.getHeader().getPositionByName(partition_col_name);
    if (partition_column_position == input.getHeader().columns())
    {
        chunk.addColumn(partition_column->cloneResized(chunk.getNumRows()));
    }
    else
    {
        chunk.addColumn(partition_column_position, partition_column->cloneResized(chunk.getNumRows()));
    }
}
}
