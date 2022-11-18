#include "CHColumnToSparkRow.h"
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}


namespace local_engine
{
using namespace DB;

int64_t calculateBitSetWidthInBytes(int32_t num_fields)
{
    return ((num_fields + 63) / 64) * 8;
}

static int64_t calculatedFixeSizePerRow(int64_t num_cols)
{
    return calculateBitSetWidthInBytes(num_cols) + num_cols * 8;
}

int64_t roundNumberOfBytesToNearestWord(int64_t num_bytes)
{
    auto remainder = num_bytes & 0x07; // This is equivalent to `numBytes % 8`
    return num_bytes + ((8 - remainder) & 0x7);
}


void bitSet(char * bitmap, int32_t index)
{
    int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
    int64_t word_offset = (index >> 6) * 8;
    int64_t word;
    memcpy(&word, bitmap + word_offset, sizeof(int64_t));
    int64_t value = word | mask;
    memcpy(bitmap + word_offset, &value, sizeof(int64_t));
}

ALWAYS_INLINE bool isBitSet(const char * bitmap, int32_t index)
{
    assert(index >= 0);
    int64_t mask = 1 << (index & 63);
    int64_t word_offset = static_cast<int64_t>(index >> 6) * 8L;
    int64_t word = *reinterpret_cast<const int64_t *>(bitmap + word_offset);
    return word & mask;
}

static void writeFixedLengthNonNullableValue(
    char * buffer_address,
    int64_t field_offset,
    const ColumnWithTypeAndName & col,
    int64_t num_rows,
    const std::vector<int64_t> & offsets)
{
    FixedLengthDataWriter writer(col.type);
    for (size_t i = 0; i < static_cast<size_t>(num_rows); i++)
        writer.unsafeWrite(col.column->getDataAt(i), buffer_address + offsets[i] + field_offset);
}

static void writeFixedLengthNullableValue(
    char * buffer_address,
    int64_t field_offset,
    const ColumnWithTypeAndName & col,
    int32_t col_index,
    int64_t num_rows,
    const std::vector<int64_t> & offsets)
{
    const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*col.column);
    const auto & null_map = nullable_column->getNullMapData();
    const auto & nested_column = nullable_column->getNestedColumn();
    FixedLengthDataWriter writer(col.type);
    for (size_t i = 0; i < static_cast<size_t>(num_rows); i++)
    {
        if (null_map[i])
            bitSet(buffer_address + offsets[i], col_index);
        else
            writer.unsafeWrite(nested_column.getDataAt(i), buffer_address + offsets[i] + field_offset);
    }
}

static void writeVariableLengthNonNullableValue(
    char * buffer_address,
    int64_t field_offset,
    const ColumnWithTypeAndName & col,
    int64_t num_rows,
    const std::vector<int64_t> & offsets,
    std::vector<int64_t> & buffer_cursor)
{
    const auto type_without_nullable{std::move(removeNullable(col.type))};
    const bool use_raw_data = BackingDataLengthCalculator::isDataTypeSupportRawData(type_without_nullable);
    VariableLengthDataWriter writer(col.type, buffer_address, offsets, buffer_cursor);
    if (use_raw_data)
    {
        for (size_t i = 0; i < static_cast<size_t>(num_rows); i++)
        {
            StringRef str = col.column->getDataAt(i);
            int64_t offset_and_size = writer.writeUnalignedBytes(i, str.data, str.size, 0);
            memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, 8);
        }
    }
    else
    {
        Field field;
        for (size_t i = 0; i < static_cast<size_t>(num_rows); i++)
        {
            field = std::move((*col.column)[i]);
            int64_t offset_and_size = writer.write(i, field, 0);
            memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, 8);
        }
    }
}

static void writeVariableLengthNullableValue(
    char * buffer_address,
    int64_t field_offset,
    const ColumnWithTypeAndName & col,
    int32_t col_index,
    int64_t num_rows,
    const std::vector<int64_t> & offsets,
    std::vector<int64_t> & buffer_cursor)
{
    const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*col.column);
    const auto & null_map = nullable_column->getNullMapData();
    const auto & nested_column = nullable_column->getNestedColumn();
    const auto type_without_nullable{std::move(removeNullable(col.type))};
    const bool use_raw_data = BackingDataLengthCalculator::isDataTypeSupportRawData(type_without_nullable);
    VariableLengthDataWriter writer(col.type, buffer_address, offsets, buffer_cursor);
    if (use_raw_data)
    {
        for (size_t i = 0; i < static_cast<size_t>(num_rows); i++)
        {
            if (null_map[i])
                bitSet(buffer_address + offsets[i], col_index);
            else
            {
                StringRef str = nested_column.getDataAt(i);
                int64_t offset_and_size = writer.writeUnalignedBytes(i, str.data, str.size, 0);
                memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, 8);
            }
        }
    }
    else
    {
        Field field;
        for (size_t i = 0; i < static_cast<size_t>(num_rows); i++)
        {
            if (null_map[i])
                bitSet(buffer_address + offsets[i], col_index);
            else
            {
                field = std::move(nested_column[i]);
                int64_t offset_and_size = writer.write(i, field, 0);
                memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, 8);
            }
        }
    }
}


static void writeValue(
    char * buffer_address,
    int64_t field_offset,
    const ColumnWithTypeAndName & col,
    int32_t col_index,
    int64_t num_rows,
    const std::vector<int64_t> & offsets,
    std::vector<int64_t> & buffer_cursor)
{
    const auto type_without_nullable{std::move(removeNullable(col.type))};
    const auto is_nullable = isColumnNullable(*col.column);
    if (BackingDataLengthCalculator::isFixedLengthDataType(type_without_nullable))
    {
        if (is_nullable)
            writeFixedLengthNullableValue(buffer_address, field_offset, col, col_index, num_rows, offsets);
        else
            writeFixedLengthNonNullableValue(buffer_address, field_offset, col, num_rows, offsets);
    }
    else if (BackingDataLengthCalculator::isVariableLengthDataType(type_without_nullable))
    {
        if (is_nullable)
            writeVariableLengthNullableValue(buffer_address, field_offset, col, col_index, num_rows, offsets, buffer_cursor);
        else
            writeVariableLengthNonNullableValue(buffer_address, field_offset, col, num_rows, offsets, buffer_cursor);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Doesn't support type {} for writeValue", col.type->getName());
}

SparkRowInfo::SparkRowInfo(const Block & block)
    : types(std::move(block.getDataTypes()))
    , num_rows(block.rows())
    , num_cols(block.columns())
    , null_bitset_width_in_bytes(calculateBitSetWidthInBytes(num_cols))
    , total_bytes(0)
    , offsets(num_rows, 0)
    , lengths(num_rows, 0)
    , buffer_cursor(num_rows, 0)
    , buffer_address(nullptr)
{
    int64_t fixed_size_per_row = calculatedFixeSizePerRow(num_cols);

    /// Initialize lengths and buffer_cursor
    for (int64_t i = 0; i < num_rows; i++)
    {
        lengths[i] = fixed_size_per_row;
        buffer_cursor[i] = fixed_size_per_row;
    }

    for (int64_t col_idx = 0; col_idx < num_cols; ++col_idx)
    {
        const auto & col = block.getByPosition(col_idx);

        /// No need to calculate backing data length for fixed length types
        const auto type_without_nullable = removeNullable(col.type);
        if (BackingDataLengthCalculator::isVariableLengthDataType(type_without_nullable))
        {
            if (BackingDataLengthCalculator::isDataTypeSupportRawData(type_without_nullable))
            {
                const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*col.column);
                if (nullable_column)
                {
                    const auto & nested_column = nullable_column->getNestedColumn();
                    const auto & null_map = nullable_column->getNullMapData();
                    for (auto row_idx = 0; row_idx < num_rows; ++row_idx)
                        if (!null_map[row_idx])
                            lengths[row_idx] += roundNumberOfBytesToNearestWord(nested_column.getDataAt(row_idx).size);
                }
                else
                {
                    for (auto row_idx = 0; row_idx < num_rows; ++row_idx)
                        lengths[row_idx] += roundNumberOfBytesToNearestWord(col.column->getDataAt(row_idx).size);
                }
            }
            else
            {
                BackingDataLengthCalculator calculator(col.type);
                for (auto row_idx = 0; row_idx < num_rows; ++row_idx)
                {
                    const auto field = (*col.column)[row_idx];
                    lengths[row_idx] += calculator.calculate(field);
                }
            }
        }
    }

    /// Initialize offsets
    for (int64_t i = 1; i < num_rows; ++i)
        offsets[i] = offsets[i - 1] + lengths[i - 1];

    /// Initialize total_bytes
    for (int64_t i = 0; i < num_rows; ++i)
        total_bytes += lengths[i];
}

const DB::DataTypes & SparkRowInfo::getDataTypes() const
{
    return types;
}

int64_t SparkRowInfo::getFieldOffset(int32_t col_idx) const
{
    return null_bitset_width_in_bytes + 8L * col_idx;
}

int64_t SparkRowInfo::getNullBitsetWidthInBytes() const
{
    return null_bitset_width_in_bytes;
}

void SparkRowInfo::setNullBitsetWidthInBytes(int64_t null_bitset_width_in_bytes_)
{
    null_bitset_width_in_bytes = null_bitset_width_in_bytes_;
}

int64_t SparkRowInfo::getNumCols() const
{
    return num_cols;
}

void SparkRowInfo::setNumCols(int64_t num_cols_)
{
    num_cols = num_cols_;
}

int64_t SparkRowInfo::getNumRows() const
{
    return num_rows;
}

void SparkRowInfo::setNumRows(int64_t num_rows_)
{
    num_rows = num_rows_;
}

char * SparkRowInfo::getBufferAddress() const
{
    return buffer_address;
}

void SparkRowInfo::setBufferAddress(char * buffer_address_)
{
    buffer_address = buffer_address_;
}

const std::vector<int64_t> & SparkRowInfo::getOffsets() const
{
    return offsets;
}

const std::vector<int64_t> & SparkRowInfo::getLengths() const
{
    return lengths;
}

std::vector<int64_t> & SparkRowInfo::getBufferCursor()
{
    return buffer_cursor;
}

int64_t SparkRowInfo::getTotalBytes() const
{
    return total_bytes;
}

std::unique_ptr<SparkRowInfo> CHColumnToSparkRow::convertCHColumnToSparkRow(const Block & block)
{
    if (!block.rows() || !block.columns())
        return {};

    std::unique_ptr<SparkRowInfo> spark_row_info = std::make_unique<SparkRowInfo>(block);
    spark_row_info->setBufferAddress(reinterpret_cast<char *>(alloc(spark_row_info->getTotalBytes(), 64)));
    // spark_row_info->setBufferAddress(alignedAlloc(spark_row_info->getTotalBytes(), 64));
    memset(spark_row_info->getBufferAddress(), 0, spark_row_info->getTotalBytes());
    for (auto col_idx = 0; col_idx < spark_row_info->getNumCols(); col_idx++)
    {
        const auto & col = block.getByPosition(col_idx);
        int64_t field_offset = spark_row_info->getFieldOffset(col_idx);

        ColumnWithTypeAndName col_not_const{col.column->convertToFullColumnIfConst(), col.type, col.name};
        writeValue(
            spark_row_info->getBufferAddress(),
            field_offset,
            col_not_const,
            col_idx,
            spark_row_info->getNumRows(),
            spark_row_info->getOffsets(),
            spark_row_info->getBufferCursor());
    }
    return spark_row_info;
}

void CHColumnToSparkRow::freeMem(char * address, size_t size)
{
    free(address, size);
    // rollback(size);
}

BackingDataLengthCalculator::BackingDataLengthCalculator(const DataTypePtr & type_)
    : type_without_nullable(removeNullable(type_)), which(type_without_nullable)
{
    if (!isFixedLengthDataType(type_without_nullable) && !isVariableLengthDataType(type_without_nullable))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Doesn't support type {} for BackingDataLengthCalculator", type_without_nullable->getName());
}

int64_t BackingDataLengthCalculator::calculate(const Field & field) const
{
    if (field.isNull())
        return 0;

    if (which.isNativeInt() || which.isNativeUInt() || which.isFloat() || which.isDateOrDate32() || which.isDateTime64()
        || which.isDecimal32() || which.isDecimal64())
        return 0;

    if (which.isStringOrFixedString())
    {
        const auto & str = field.get<String>();
        return roundNumberOfBytesToNearestWord(str.size());
    }

    if (which.isDecimal128())
        return 16;

    if (which.isArray())
    {
        /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing buffer
        const auto & array = field.get<Array>(); /// Array can not be wrapped with Nullable
        const auto num_elems = array.size();
        int64_t res = 8 + calculateBitSetWidthInBytes(num_elems);

        const auto * array_type = typeid_cast<const DataTypeArray *>(type_without_nullable.get());
        const auto & nested_type = array_type->getNestedType();
        res += roundNumberOfBytesToNearestWord(getArrayElementSize(nested_type) * num_elems);

        BackingDataLengthCalculator calculator(nested_type);
        for (size_t i = 0; i < array.size(); ++i)
            res += calculator.calculate(array[i]);
        return res;
    }

    if (which.isMap())
    {
        /// 内存布局：Length of UnsafeArrayData of key(8B) |  UnsafeArrayData of key | UnsafeArrayData of value
        int64_t res = 8;

        /// Construct Array of keys and values from Map
        const auto & map = field.get<Map>(); /// Map can not be wrapped with Nullable
        const auto num_keys = map.size();
        auto array_key = Array();
        auto array_val = Array();
        array_key.reserve(num_keys);
        array_val.reserve(num_keys);
        for (size_t i = 0; i < num_keys; ++i)
        {
            const auto & pair = map[i].get<DB::Tuple>();
            array_key.push_back(pair[0]);
            array_val.push_back(pair[1]);
        }

        const auto * map_type = typeid_cast<const DB::DataTypeMap *>(type_without_nullable.get());

        const auto & key_type = map_type->getKeyType();
        const auto key_array_type = std::make_shared<DataTypeArray>(key_type);
        BackingDataLengthCalculator calculator_key(key_array_type);
        res += calculator_key.calculate(array_key);

        const auto & val_type = map_type->getValueType();
        const auto type_array_val = std::make_shared<DataTypeArray>(val_type);
        BackingDataLengthCalculator calculator_val(type_array_val);
        res += calculator_val.calculate(array_val);
        return res;
    }

    if (which.isTuple())
    {
        /// 内存布局：null_bitmap(字节数与字段数成正比) | field1 value(8B) | field2 value(8B) | ... | fieldn value(8B) | backing buffer
        const auto & tuple = field.get<Tuple>(); /// Tuple can not be wrapped with Nullable
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_without_nullable.get());
        const auto & type_fields = type_tuple->getElements();
        const auto num_fields = type_fields.size();
        int64_t res = calculateBitSetWidthInBytes(num_fields) + 8 * num_fields;
        for (size_t i = 0; i < num_fields; ++i)
        {
            BackingDataLengthCalculator calculator(type_fields[i]);
            res += calculator.calculate(tuple[i]);
        }
        return res;
    }

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "Doesn't support type {} for BackingBufferLengthCalculator", type_without_nullable->getName());
}

int64_t BackingDataLengthCalculator::getArrayElementSize(const DataTypePtr & nested_type)
{
    const WhichDataType nested_which(removeNullable(nested_type));
    if (nested_which.isUInt8() || nested_which.isInt8())
        return 1;
    else if (nested_which.isUInt16() || nested_which.isInt16() || nested_which.isDate())
        return 2;
    else if (
        nested_which.isUInt32() || nested_which.isInt32() || nested_which.isFloat32() || nested_which.isDate32()
        || nested_which.isDecimal32())
        return 4;
    else if (
        nested_which.isUInt64() || nested_which.isInt64() || nested_which.isFloat64() || nested_which.isDateTime64()
        || nested_which.isDecimal64())
        return 8;
    else
        return 8;
}

bool BackingDataLengthCalculator::isFixedLengthDataType(const DataTypePtr & type_without_nullable)
{
    const WhichDataType which(type_without_nullable);
    return which.isUInt8() || which.isInt8() || which.isUInt16() || which.isInt16() || which.isDate() || which.isUInt32() || which.isInt32()
        || which.isFloat32() || which.isDate32() || which.isDecimal32() || which.isUInt64() || which.isInt64() || which.isFloat64()
        || which.isDateTime64() || which.isDecimal64();
}

bool BackingDataLengthCalculator::isVariableLengthDataType(const DataTypePtr & type_without_nullable)
{
    const WhichDataType which(type_without_nullable);
    return which.isStringOrFixedString() || which.isDecimal128() || which.isArray() || which.isMap() || which.isTuple();
}

bool BackingDataLengthCalculator::isDataTypeSupportRawData(const DB::DataTypePtr & type_without_nullable)
{
    const WhichDataType which(type_without_nullable);
    return isFixedLengthDataType(type_without_nullable) || which.isStringOrFixedString() || which.isDecimal128();
}


VariableLengthDataWriter::VariableLengthDataWriter(
    const DataTypePtr & type_, char * buffer_address_, const std::vector<int64_t> & offsets_, std::vector<int64_t> & buffer_cursor_)
    : type_without_nullable(removeNullable(type_))
    , which(type_without_nullable)
    , buffer_address(buffer_address_)
    , offsets(offsets_)
    , buffer_cursor(buffer_cursor_)
{
    assert(buffer_address);
    assert(!offsets.empty());
    assert(!buffer_cursor.empty());
    assert(offsets.size() == buffer_cursor.size());

    if (!BackingDataLengthCalculator::isVariableLengthDataType(type_without_nullable))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataWriter doesn't support type {}", type_without_nullable->getName());
}

int64_t VariableLengthDataWriter::writeArray(size_t row_idx, const DB::Array & array, int64_t parent_offset)
{
    /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing data
    const auto & offset = offsets[row_idx];
    auto & cursor = buffer_cursor[row_idx];
    const auto num_elems = array.size();
    const auto * array_type = typeid_cast<const DataTypeArray *>(type_without_nullable.get());
    const auto & nested_type = array_type->getNestedType();

    /// Write numElements(8B)
    const auto start = cursor;
    memcpy(buffer_address + offset + cursor, &num_elems, 8);
    cursor += 8;
    if (num_elems == 0)
        return BackingDataLengthCalculator::getOffsetAndSize(start - parent_offset, 8);

    /// Skip null_bitmap(already reset to zero)
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_elems);
    cursor += len_null_bitmap;

    /// Skip values(already reset to zero)
    const auto elem_size = BackingDataLengthCalculator::getArrayElementSize(nested_type);
    const auto len_values = roundNumberOfBytesToNearestWord(elem_size * num_elems);
    cursor += len_values;

    if (BackingDataLengthCalculator::isFixedLengthDataType(removeNullable(nested_type)))
    {
        /// If nested type is fixed-length data type, update null_bitmap and values in place
        FixedLengthDataWriter writer(nested_type);
        for (size_t i = 0; i < num_elems; ++i)
        {
            const auto & elem = array[i];
            if (elem.isNull())
                bitSet(buffer_address + offset + start + 8, i);
            else
                // writer.write(elem, buffer_address + offset + start + 8 + len_null_bitmap + i * elem_size);
                writer.unsafeWrite(&elem.reinterpret<char>(), buffer_address + offset + start + 8 + len_null_bitmap + i * elem_size);
        }
    }
    else
    {
        /// If nested type is not fixed-length data type, update null_bitmap in place
        /// And append values in backing data recursively
        VariableLengthDataWriter writer(nested_type, buffer_address, offsets, buffer_cursor);
        for (size_t i = 0; i < num_elems; ++i)
        {
            const auto & elem = array[i];
            if (elem.isNull())
                bitSet(buffer_address + offset + start + 8, i);
            else
            {
                const auto offset_and_size = writer.write(row_idx, elem, start);
                memcpy(buffer_address + offset + start + 8 + len_null_bitmap + i * elem_size, &offset_and_size, 8);
            }
        }
    }
    return BackingDataLengthCalculator::getOffsetAndSize(start - parent_offset, cursor - start);
}

int64_t VariableLengthDataWriter::writeMap(size_t row_idx, const DB::Map & map, int64_t parent_offset)
{
    /// 内存布局：Length of UnsafeArrayData of key(8B) |  UnsafeArrayData of key | UnsafeArrayData of value
    const auto & offset = offsets[row_idx];
    auto & cursor = buffer_cursor[row_idx];

    /// Skip length of UnsafeArrayData of key(8B)
    const auto start = cursor;
    cursor += 8;

    /// If Map is empty, return in advance
    const auto num_pairs = map.size();
    if (num_pairs == 0)
        return BackingDataLengthCalculator::getOffsetAndSize(start - parent_offset, 8);

    /// Construct array of keys and array of values from map
    auto key_array = Array();
    auto val_array = Array();
    key_array.reserve(num_pairs);
    val_array.reserve(num_pairs);
    for (size_t i = 0; i < num_pairs; ++i)
    {
        const auto & pair = map[i].get<DB::Tuple>();
        key_array.push_back(pair[0]);
        val_array.push_back(pair[1]);
    }

    const auto * map_type = typeid_cast<const DB::DataTypeMap *>(type_without_nullable.get());

    /// Append UnsafeArrayData of key
    const auto & key_type = map_type->getKeyType();
    const auto key_array_type = std::make_shared<DataTypeArray>(key_type);
    VariableLengthDataWriter key_writer(key_array_type, buffer_address, offsets, buffer_cursor);
    const auto key_array_size = BackingDataLengthCalculator::extractSize(key_writer.write(row_idx, key_array, start + 8));

    /// Fill length of UnsafeArrayData of key
    memcpy(buffer_address + offset + start, &key_array_size, 8);

    /// Append UnsafeArrayData of value
    const auto & val_type = map_type->getValueType();
    const auto val_array_type = std::make_shared<DataTypeArray>(val_type);
    VariableLengthDataWriter val_writer(val_array_type, buffer_address, offsets, buffer_cursor);
    val_writer.write(row_idx, val_array, start + 8 + key_array_size);
    return BackingDataLengthCalculator::getOffsetAndSize(start - parent_offset, cursor - start);
}

int64_t VariableLengthDataWriter::writeStruct(size_t row_idx, const DB::Tuple & tuple, int64_t parent_offset)
{
    /// 内存布局：null_bitmap(字节数与字段数成正比) | values(num_fields * 8B) | backing data
    const auto & offset = offsets[row_idx];
    auto & cursor = buffer_cursor[row_idx];
    const auto start = cursor;

    /// Skip null_bitmap
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type_without_nullable.get());
    const auto & field_types = tuple_type->getElements();
    const auto num_fields = field_types.size();
    if (num_fields == 0)
        return BackingDataLengthCalculator::getOffsetAndSize(start - parent_offset, 0);
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_fields);
    cursor += len_null_bitmap;

    /// Skip values
    cursor += num_fields * 8;

    /// If field type is fixed-length, fill field value in values region
    /// else append it to backing data region, and update offset_and_size in values region
    for (size_t i = 0; i < num_fields; ++i)
    {
        const auto & field_value = tuple[i];
        const auto & field_type = field_types[i];
        if (field_value.isNull())
        {
            bitSet(buffer_address + offset + start, i);
            continue;
        }

        if (BackingDataLengthCalculator::isFixedLengthDataType(removeNullable(field_type)))
        {
            FixedLengthDataWriter writer(field_type);
            // writer.write(field_value, buffer_address + offset + start + len_null_bitmap + i * 8);
            writer.unsafeWrite(&field_value.reinterpret<char>(), buffer_address + offset + start + len_null_bitmap + i * 8);
        }
        else
        {
            VariableLengthDataWriter writer(field_type, buffer_address, offsets, buffer_cursor);
            const auto offset_and_size = writer.write(row_idx, field_value, start);
            memcpy(buffer_address + offset + start + len_null_bitmap + 8 * i, &offset_and_size, 8);
        }
    }
    return BackingDataLengthCalculator::getOffsetAndSize(start - parent_offset, cursor - start);
}

int64_t VariableLengthDataWriter::write(size_t row_idx, const DB::Field & field, int64_t parent_offset)
{
    assert(row_idx < offsets.size());

    if (field.isNull())
        return 0;

    if (which.isStringOrFixedString())
    {
        const auto & str = field.get<String>();
        return writeUnalignedBytes(row_idx, str.data(), str.size(), parent_offset);
    }

    if (which.isDecimal128())
    {
        // const auto & decimal = field.get<DecimalField<Decimal128>>();
        // const auto value = decimal.getValue();
        return writeUnalignedBytes(row_idx, &field.reinterpret<char>(), sizeof(Decimal128), parent_offset);
    }

    if (which.isArray())
    {
        const auto & array = field.get<Array>();
        return writeArray(row_idx, array, parent_offset);
    }

    if (which.isMap())
    {
        const auto & map = field.get<Map>();
        return writeMap(row_idx, map, parent_offset);
    }

    if (which.isTuple())
    {
        const auto & tuple = field.get<Tuple>();
        return writeStruct(row_idx, tuple, parent_offset);
    }

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "Doesn't support type {} for BackingDataWriter", type_without_nullable->getName());
}

int64_t BackingDataLengthCalculator::getOffsetAndSize(int64_t cursor, int64_t size)
{
    return (cursor << 32) | size;
}

int64_t BackingDataLengthCalculator::extractOffset(int64_t offset_and_size)
{
    return offset_and_size >> 32;
}

int64_t BackingDataLengthCalculator::extractSize(int64_t offset_and_size)
{
    return offset_and_size & 0xffffffff;
}

int64_t VariableLengthDataWriter::writeUnalignedBytes(size_t row_idx, const char * src, size_t size, int64_t parent_offset)
{
    memcpy(buffer_address + offsets[row_idx] + buffer_cursor[row_idx], src, size);
    auto res = BackingDataLengthCalculator::getOffsetAndSize(buffer_cursor[row_idx] - parent_offset, size);
    buffer_cursor[row_idx] += roundNumberOfBytesToNearestWord(size);
    return res;
}


FixedLengthDataWriter::FixedLengthDataWriter(const DB::DataTypePtr & type_)
    : type_without_nullable(removeNullable(type_)), which(type_without_nullable)
{
    if (!BackingDataLengthCalculator::isFixedLengthDataType(type_without_nullable))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "FixedLengthWriter doesn't support type {}", type_without_nullable->getName());
}

void FixedLengthDataWriter::write(const DB::Field & field, char * buffer)
{
    /// Skip null value
    if (field.isNull())
        return;

    if (which.isUInt8())
    {
        const auto value = UInt8(field.get<UInt8>());
        memcpy(buffer, &value, 1);
    }
    else if (which.isUInt16() || which.isDate())
    {
        const auto value = UInt16(field.get<UInt16>());
        memcpy(buffer, &value, 2);
    }
    else if (which.isUInt32() || which.isDate32())
    {
        const auto value = UInt32(field.get<UInt32>());
        memcpy(buffer, &value, 4);
    }
    else if (which.isUInt64())
    {
        const auto & value = field.get<UInt64>();
        memcpy(buffer, &value, 8);
    }
    else if (which.isInt8())
    {
        const auto value = Int8(field.get<Int8>());
        memcpy(buffer, &value, 1);
    }
    else if (which.isInt16())
    {
        const auto value = Int16(field.get<Int16>());
        memcpy(buffer, &value, 2);
    }
    else if (which.isInt32())
    {
        const auto value = Int32(field.get<Int32>());
        memcpy(buffer, &value, 4);
    }
    else if (which.isInt64())
    {
        const auto & value = field.get<Int64>();
        memcpy(buffer, &value, 8);
    }
    else if (which.isFloat32())
    {
        const auto value = Float32(field.get<Float32>());
        memcpy(buffer, &value, 4);
    }
    else if (which.isFloat64())
    {
        const auto & value = field.get<Float64>();
        memcpy(buffer, &value, 8);
    }
    else if (which.isDecimal32())
    {
        const auto & value = field.get<Decimal32>();
        const auto decimal = value.getValue();
        memcpy(buffer, &decimal, 4);
    }
    else if (which.isDecimal64() || which.isDateTime64())
    {
        const auto & value = field.get<Decimal64>();
        auto decimal = value.getValue();
        memcpy(buffer, &decimal, 8);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "FixedLengthDataWriter doesn't support type {}", type_without_nullable->getName());
}

void FixedLengthDataWriter::unsafeWrite(const StringRef & str, char * buffer)
{
    memcpy(buffer, str.data, str.size);
}

void FixedLengthDataWriter::unsafeWrite(const char * __restrict src, char * __restrict buffer)
{
    memcpy(buffer, src, type_without_nullable->getSizeOfValueInMemory());
}

}
