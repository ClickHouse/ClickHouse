#include "CHColumnToSparkRow.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

#define WRITE_VECTOR_COLUMN(TYPE, PRIME_TYPE, GETTER) \
    const auto * type_col = checkAndGetColumn<ColumnVector<TYPE>>(*nested_col); \
    for (auto i = 0; i < num_rows; i++) \
    { \
        bool is_null = nullable_column && nullable_column->isNullAt(i); \
        if (is_null) \
        { \
            setNullAt(buffer_address, offsets[i], field_offset, col_index); \
        } \
        else \
        { \
            auto * pointer = reinterpret_cast<PRIME_TYPE *>(buffer_address + offsets[i] + field_offset); \
            pointer[0] = type_col->GETTER(i);\
        } \
    }

namespace local_engine
{
using namespace DB;
int64_t calculateBitSetWidthInBytes(int32_t num_fields)
{
    return ((num_fields + 63) / 64) * 8;
}

int64_t calculatedFixeSizePerRow(DB::Block & header, int64_t num_cols)
{
    auto fields = header.getNamesAndTypesList();
    // Calculate the decimal col num when the precision >18
    int32_t count = 0;
    for (auto i = 0; i < num_cols; i++)
    {
        auto type = removeNullable(fields.getTypes()[i]);
        DB::WhichDataType which(type);
        if (which.isDecimal128())
        {
            const auto & dtype = typeid_cast<const DataTypeDecimal<Decimal128> *>(type.get());
            int32_t precision = dtype->getPrecision();
            if (precision > 18)
                count++;
        }
    }

    int64_t fixed_size = calculateBitSetWidthInBytes(num_cols) + num_cols * 8;
    int64_t decimal_cols_size = count * 16;
    return fixed_size + decimal_cols_size;
}

int64_t roundNumberOfBytesToNearestWord(int64_t numBytes)
{
    int64_t remainder = numBytes & 0x07; // This is equivalent to `numBytes % 8`
    if (remainder == 0)
    {
        return numBytes;
    }
    else
    {
        return numBytes + (8 - remainder);
    }
}

int64_t getFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index)
{
    return nullBitsetWidthInBytes + 8L * index;
}

void bitSet(uint8_t * buffer_address, int32_t index)
{
    int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
    int64_t word_offset = (index >> 6) * 8;
    int64_t word;
    memcpy(&word, buffer_address + word_offset, sizeof(int64_t));
    int64_t value = word | mask;
    memcpy(buffer_address + word_offset, &value, sizeof(int64_t));
}

void setNullAt(uint8_t * buffer_address, int64_t row_offset, int64_t field_offset, int32_t col_index)
{
    bitSet(buffer_address + row_offset, col_index);
    // set the value to 0
    memset(buffer_address + row_offset + field_offset, 0, sizeof(int64_t));
}

void writeValue(
    uint8_t * buffer_address,
    int64_t field_offset,
    ColumnWithTypeAndName & col,
    int32_t col_index,
    int64_t num_rows,
    std::vector<int64_t> & offsets,
    std::vector<int64_t> & buffer_cursor)
{
    ColumnPtr nested_col = col.column;
    const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*col.column);
    if (nullable_column)
    {
        nested_col = nullable_column->getNestedColumnPtr();
    }
    nested_col = nested_col->convertToFullColumnIfConst();
    WhichDataType which(nested_col->getDataType());
    if (which.isUInt8())
    {
        WRITE_VECTOR_COLUMN(UInt8, uint8_t, getInt)
    }
    else if (which.isInt8())
    {
        WRITE_VECTOR_COLUMN(Int8, int8_t, getInt)
    }
    else if (which.isInt16())
    {
        WRITE_VECTOR_COLUMN(Int16, int16_t, getInt)
    }
    else if (which.isUInt16())
    {
        WRITE_VECTOR_COLUMN(UInt16, uint16_t , get64)
    }
    else if (which.isInt32())
    {
        WRITE_VECTOR_COLUMN(Int32, int32_t, getInt)
    }
    else if (which.isInt64())
    {
        WRITE_VECTOR_COLUMN(Int64, int64_t, getInt)
    }
    else if (which.isUInt64())
    {
        WRITE_VECTOR_COLUMN(UInt64, int64_t, get64)
    }
    else if (which.isFloat32())
    {
        WRITE_VECTOR_COLUMN(Float32, float_t, getFloat32)
    }
    else if (which.isFloat64())
    {
        WRITE_VECTOR_COLUMN(Float64, double_t, getFloat64)
    }
    else if (which.isDate())
    {
        WRITE_VECTOR_COLUMN(UInt16, uint16_t, get64)
    }
    else if (which.isDate32())
    {
        WRITE_VECTOR_COLUMN(UInt32, uint32_t, get64)
    }
    else if (which.isDateTime64())
    {
        using ColumnDateTime64 = ColumnDecimal<DateTime64>;
        const auto * datetime64_col = checkAndGetColumn<ColumnDateTime64>(*nested_col);
        for (auto i=0; i<num_rows; i++)
        {
            bool is_null = nullable_column && nullable_column->isNullAt(i);
            if (is_null)
            {
                setNullAt(buffer_address, offsets[i], field_offset, col_index);
            }
            else
            {
                auto * pointer = reinterpret_cast<int64_t *>(buffer_address + offsets[i] + field_offset);
                pointer[0] = datetime64_col->getInt(i);
            }
        }
    }
    else if (which.isString())
    {
        const auto * string_col = checkAndGetColumn<ColumnString>(*nested_col);
        for (auto i = 0; i < num_rows; i++)
        {
            bool is_null = nullable_column && nullable_column->isNullAt(i);
            if (is_null)
            {
                setNullAt(buffer_address, offsets[i], field_offset, col_index);
            }
            else
            {
                StringRef string_value = string_col->getDataAt(i);
                // write the variable value
                memcpy(buffer_address + offsets[i] + buffer_cursor[i], string_value.data, string_value.size);
                // write the offset and size
                int64_t offset_and_size = (buffer_cursor[i] << 32) | string_value.size;
                memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, sizeof(int64_t));
                buffer_cursor[i] += string_value.size;
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support type {} convert from ch to spark" ,magic_enum::enum_name(nested_col->getDataType()));
    }
}

SparkRowInfo::SparkRowInfo(DB::Block & block)
{
    num_rows = block.rows();
    num_cols = block.columns();
    null_bitset_width_in_bytes = calculateBitSetWidthInBytes(num_cols);
    int64_t fixed_size_per_row = calculatedFixeSizePerRow(block, num_cols);
    // Initialize the offsets_ , lengths_, buffer_cursor_
    for (auto i = 0; i < num_rows; i++)
    {
        lengths.push_back(fixed_size_per_row);
        offsets.push_back(0);
        buffer_cursor.push_back(null_bitset_width_in_bytes + 8 * num_cols);
    }
    // Calculated the lengths_
    for (auto i = 0; i < num_cols; i++)
    {
        auto col = block.getByPosition(i);
        if (isStringOrFixedString(removeNullable(col.type)))
        {
            size_t length;
            for (auto j = 0; j < num_rows; j++)
            {
                length = col.column->getDataAt(j).size;
                lengths[j] += roundNumberOfBytesToNearestWord(length);
            }
        }
    }
}

int64_t local_engine::SparkRowInfo::getNullBitsetWidthInBytes() const
{
    return null_bitset_width_in_bytes;
}

void local_engine::SparkRowInfo::setNullBitsetWidthInBytes(int64_t null_bitset_width_in_bytes_)
{
    null_bitset_width_in_bytes = null_bitset_width_in_bytes_;
}
int64_t local_engine::SparkRowInfo::getNumCols() const
{
    return num_cols;
}
void local_engine::SparkRowInfo::setNumCols(int64_t numCols)
{
    num_cols = numCols;
}
int64_t local_engine::SparkRowInfo::getNumRows() const
{
    return num_rows;
}
void local_engine::SparkRowInfo::setNumRows(int64_t numRows)
{
    num_rows = numRows;
}
unsigned char * local_engine::SparkRowInfo::getBufferAddress() const
{
    return buffer_address;
}
void local_engine::SparkRowInfo::setBufferAddress(unsigned char * bufferAddress)
{
    buffer_address = bufferAddress;
}
const std::vector<int64_t> & local_engine::SparkRowInfo::getOffsets() const
{
    return offsets;
}
const std::vector<int64_t> & local_engine::SparkRowInfo::getLengths() const
{
    return lengths;
}
int64_t SparkRowInfo::getTotalBytes() const
{
    return total_bytes;
}
std::unique_ptr<SparkRowInfo> local_engine::CHColumnToSparkRow::convertCHColumnToSparkRow(Block & block)
{
    std::unique_ptr<SparkRowInfo> spark_row_info = std::make_unique<SparkRowInfo>(block);
    // Calculated the offsets_  and total memory size based on lengths_
    int64_t total_memory_size = spark_row_info->lengths[0];
    for (auto i = 1; i < spark_row_info->num_rows; i++)
    {
        spark_row_info->offsets[i] = spark_row_info->offsets[i - 1] + spark_row_info->lengths[i - 1];
        total_memory_size += spark_row_info->lengths[i];
    }
    spark_row_info->total_bytes = total_memory_size;
    spark_row_info->buffer_address = reinterpret_cast<unsigned char *>(alloc(total_memory_size));
    memset(spark_row_info->buffer_address, 0, sizeof(int8_t) * spark_row_info->total_bytes);
    for (auto i = 0; i < spark_row_info->num_cols; i++)
    {
        auto array = block.getByPosition(i);
        int64_t field_offset = getFieldOffset(spark_row_info->null_bitset_width_in_bytes, i);
        writeValue(
            spark_row_info->buffer_address,
            field_offset,
            array,
            i,
            spark_row_info->num_rows,
            spark_row_info->offsets,
            spark_row_info->buffer_cursor);
    }
    return spark_row_info;
}
void CHColumnToSparkRow::freeMem(uint8_t * address, size_t size)
{
    free(address, size);
}
}
