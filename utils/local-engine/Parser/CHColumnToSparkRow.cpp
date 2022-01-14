#include "CHColumnToSparkRow.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>

#define WRITE_VECTOR_COLUMN(TYPE, PRIME_TYPE, GETTER) \
    const auto * type_col = checkAndGetColumn<ColumnVector<TYPE>>(*col.column); \
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
        auto type = fields.getTypes()[i];
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
    WhichDataType which(nested_col->getDataType());
    if (which.isUInt8())
    {
        WRITE_VECTOR_COLUMN(UInt8, uint8_t, get64)
    }
    else if (which.isInt8())
    {
        WRITE_VECTOR_COLUMN(Int8, int8_t, get64)
    }
    else if (which.isInt16())
    {
        WRITE_VECTOR_COLUMN(Int16, int16_t, get64)
    }
    else if (which.isUInt16())
    {
        WRITE_VECTOR_COLUMN(UInt16, uint16_t , get64)
    }
    else if (which.isInt32())
    {
        WRITE_VECTOR_COLUMN(Int32, int32_t, get64)
    }
    else if (which.isInt64())
    {
        WRITE_VECTOR_COLUMN(Int64, int64_t, get64)
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
    else if (which.isString())
    {
        const auto * string_col = checkAndGetColumn<ColumnString>(*col.column);
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
        throw std::runtime_error("doesn't support type " + std::string(getTypeName(nested_col->getDataType())));
    }
}

SparkRowInfo::SparkRowInfo(DB::Block & block)
{
    num_rows_ = block.rows();
    num_cols_ = block.columns();
    nullBitsetWidthInBytes_ = calculateBitSetWidthInBytes(num_cols_);
    int64_t fixed_size_per_row = calculatedFixeSizePerRow(block, num_cols_);
    // Initialize the offsets_ , lengths_, buffer_cursor_
    for (auto i = 0; i < num_rows_; i++)
    {
        lengths_.push_back(fixed_size_per_row);
        offsets_.push_back(0);
        buffer_cursor_.push_back(nullBitsetWidthInBytes_ + 8 * num_cols_);
    }
    // Calculated the lengths_
    for (auto i = 0; i < num_cols_; i++)
    {
        auto col = block.getByPosition(i);
        if (isStringOrFixedString(col.type))
        {
            size_t length;
            for (auto j = 0; j < num_rows_; j++)
            {
                length = col.column->getDataAt(j).size;
                lengths_[j] += roundNumberOfBytesToNearestWord(length);
            }
        }
    }
}

int64_t local_engine::SparkRowInfo::getNullBitsetWidthInBytes() const
{
    return nullBitsetWidthInBytes_;
}

void local_engine::SparkRowInfo::setNullBitsetWidthInBytes(int64_t nullBitsetWidthInBytes)
{
    nullBitsetWidthInBytes_ = nullBitsetWidthInBytes;
}
int64_t local_engine::SparkRowInfo::getNumCols() const
{
    return num_cols_;
}
void local_engine::SparkRowInfo::setNumCols(int64_t numCols)
{
    num_cols_ = numCols;
}
int64_t local_engine::SparkRowInfo::getNumRows() const
{
    return num_rows_;
}
void local_engine::SparkRowInfo::setNumRows(int64_t numRows)
{
    num_rows_ = numRows;
}
unsigned char * local_engine::SparkRowInfo::getBufferAddress() const
{
    return buffer_address_;
}
void local_engine::SparkRowInfo::setBufferAddress(unsigned char * bufferAddress)
{
    buffer_address_ = bufferAddress;
}
const std::vector<int64_t> & local_engine::SparkRowInfo::getOffsets() const
{
    return offsets_;
}
const std::vector<int64_t> & local_engine::SparkRowInfo::getLengths() const
{
    return lengths_;
}
int64_t SparkRowInfo::getTotalBytes() const
{
    return total_bytes_;
}
std::unique_ptr<SparkRowInfo> local_engine::CHColumnToSparkRow::convertCHColumnToSparkRow(Block & block)
{
    std::unique_ptr<SparkRowInfo> spark_row_info = std::make_unique<SparkRowInfo>(block);
    // Calculated the offsets_  and total memory size based on lengths_
    int64_t total_memory_size = spark_row_info->lengths_[0];
    for (auto i = 1; i < spark_row_info->num_rows_; i++)
    {
        spark_row_info->offsets_[i] = spark_row_info->offsets_[i - 1] + spark_row_info->lengths_[i - 1];
        total_memory_size += spark_row_info->lengths_[i];
    }
    spark_row_info->total_bytes_ = total_memory_size;
    spark_row_info->buffer_address_ = reinterpret_cast<unsigned char *>(alloc(total_memory_size));
    memset(spark_row_info->buffer_address_, 0, sizeof(int8_t) * spark_row_info->total_bytes_);
    for (auto i = 0; i < spark_row_info->num_cols_; i++)
    {
        auto array = block.getByPosition(i);
        int64_t field_offset = getFieldOffset(spark_row_info->nullBitsetWidthInBytes_, i);
        writeValue(
            spark_row_info->buffer_address_,
            field_offset,
            array,
            i,
            spark_row_info->num_rows_,
            spark_row_info->offsets_,
            spark_row_info->buffer_cursor_);
    }
    return spark_row_info;
}
void CHColumnToSparkRow::freeMem(uint8_t * address, size_t size)
{
    free(address, size);
}
}
