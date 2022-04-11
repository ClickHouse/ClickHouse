#pragma once

#include <Core/Block.h>
#include <common/StringRef.h>
#include <Parser/CHColumnToSparkRow.h>

namespace local_engine
{
using namespace DB;



class SparkColumnToCHColumn
{
public:
    std::unique_ptr<Block> convertCHColumnToSparkRow(SparkRowInfo & spark_row_info, Block& header);
};
}

class SparkRowReader
{
public:

    bool isSet(int index)
    {
        assert(index >= 0);
        int64_t mask = 1 << (index & 63);
        int64_t word_offset = base_offset + static_cast<int64_t>(index >> 6) * 8L;
        int64_t word = *reinterpret_cast<int64_t *>(word_offset);
        return (word & mask) != 0;
    }

    inline void assertIndexIsValid(int index) const
    {
        assert(index >= 0);
        assert(index < num_fields);
    }

    bool isNullAt(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return isSet(ordinal);
    }

    int8_t getByte(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int8_t *>(getFieldOffset(ordinal));
    }

    uint8_t getUnsignedByte(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint8_t *>(getFieldOffset(ordinal));
    }


    int16_t getShort(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int16_t *>(getFieldOffset(ordinal));
    }

    uint16_t getUnsignedShort(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint16_t *>(getFieldOffset(ordinal));
    }

    int32_t getInt(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int32_t *>(getFieldOffset(ordinal));
    }

    uint32_t getUnsignedInt(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint32_t *>(getFieldOffset(ordinal));
    }

    int64_t getLong(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int64_t *>(getFieldOffset(ordinal));
    }

    float_t getFloat(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<float_t *>(getFieldOffset(ordinal));
    }

    double_t getDouble(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<double_t *>(getFieldOffset(ordinal));
    }

    StringRef getString(int ordinal)
    {
        assertIndexIsValid(ordinal);
        int64_t offset_and_size = getLong(ordinal);
        int32_t offset = static_cast<int32_t>(offset_and_size >> 32);
        int32_t size = static_cast<int32_t>(offset_and_size);
        return StringRef(reinterpret_cast<char *>(this->base_offset + offset), size);
    }

    int32_t getStringSize(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return static_cast<int32_t>(getLong(ordinal));
    }

    void pointTo(int64_t base_offset_, int32_t size_in_bytes_)
    {
        this->base_offset = base_offset_;
        this->size_in_bytes = size_in_bytes_;
    }

    explicit SparkRowReader(int32_t numFields)
        : num_fields(numFields)
    {
        this->bit_set_width_in_bytes = local_engine::calculateBitSetWidthInBytes(numFields);
    }

private:
    int64_t getFieldOffset(int ordinal) const
    {
        return base_offset + bit_set_width_in_bytes + ordinal * 8L;
    }

    int64_t base_offset;
    int32_t num_fields;
    int32_t size_in_bytes;
    int32_t bit_set_width_in_bytes;
};
