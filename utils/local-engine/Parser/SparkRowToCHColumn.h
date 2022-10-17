#pragma once

#include <memory>
#include <jni.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parser/CHColumnToSparkRow.h>
#include <base/StringRef.h>
#include <Common/JNIUtils.h>
#include <jni/jni_common.h>

namespace local_engine
{
using namespace DB;
using namespace std;


struct SparkRowToCHColumnHelper
{
    SparkRowToCHColumnHelper(vector<string>& names, vector<string>& types, vector<bool>& isNullables)
    {
        internal_cols = std::make_unique<std::vector<ColumnWithTypeAndName>>();
        internal_cols->reserve(names.size());
        typePtrs = std::make_unique<std::vector<DataTypePtr>>();
        typePtrs->reserve(names.size());
        for (size_t i = 0; i < names.size(); ++i)
        {
            const auto & name = names[i];
            const auto & type = types[i];
            const bool is_nullable = isNullables[i];
            auto data_type = parseType(type, is_nullable);
            internal_cols->push_back(ColumnWithTypeAndName(data_type, name));
            typePtrs->push_back(data_type);
        }
        header = std::make_shared<Block>(*std::move(internal_cols));
        resetWrittenColumns();
    }

    unique_ptr<vector<ColumnWithTypeAndName>> internal_cols; //for headers
    unique_ptr<vector<MutableColumnPtr>> cols;
    unique_ptr<vector<DataTypePtr>> typePtrs;
    shared_ptr<Block> header;

    void resetWrittenColumns()
    {
        cols = make_unique<vector<MutableColumnPtr>>();
        for (size_t i = 0; i < internal_cols->size(); i++)
        {
            cols->push_back(internal_cols->at(i).type->createColumn());
        }
    }

    static DataTypePtr inline wrapNullableType(bool isNullable, DataTypePtr nested_type)
    {
        if (isNullable)
        {
            return std::make_shared<DataTypeNullable>(nested_type);
        }
        else
        {
            return nested_type;
        }
    }

    //parse Spark type name to CH DataType
    DataTypePtr parseType(const string & type, const bool isNullable)
    {
        DataTypePtr internal_type = nullptr;
        auto & factory = DataTypeFactory::instance();
        if ("boolean" == type)
        {
            internal_type = factory.get("UInt8");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("byte" == type)
        {
            internal_type = factory.get("Int8");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("short" == type)
        {
            internal_type = factory.get("Int16");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("integer" == type)
        {
            internal_type = factory.get("Int32");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("long" == type)
        {
            internal_type = factory.get("Int64");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("string" == type)
        {
            internal_type = factory.get("String");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("float" == type)
        {
            internal_type = factory.get("Float32");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("double" == type)
        {
            internal_type = factory.get("Float64");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("date" == type)
        {
            internal_type = factory.get("Date32");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("timestamp" == type)
        {
            internal_type = factory.get("DateTime64(6)");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else
            throw Exception(0, "doesn't support spark type {}", type);

        return internal_type;
    }
};

class SparkRowToCHColumn
{
public:
    static jclass spark_row_interator_class;
    static jmethodID spark_row_interator_hasNext;
    static jmethodID spark_row_interator_next;
    static jmethodID spark_row_iterator_nextBatch;

    // case 1: rows are batched (this is often directly converted from Block)
    static std::unique_ptr<Block> convertSparkRowInfoToCHColumn(SparkRowInfo & spark_row_info, Block & header);

    // case 2: provided with a sequence of spark UnsafeRow, convert them to a Block
    static Block* convertSparkRowItrToCHColumn(jobject java_iter, vector<string>& names, vector<string>& types, vector<bool>& isNullables)
    {
        SparkRowToCHColumnHelper helper(names, types, isNullables);

        int attached;
        JNIEnv * env = JNIUtils::getENV(&attached);
        while (safeCallBooleanMethod(env, java_iter, spark_row_interator_hasNext))
        {
            jobject rows_buf = safeCallObjectMethod(env, java_iter, spark_row_iterator_nextBatch);
            auto * rows_buf_ptr = static_cast<char*>(env->GetDirectBufferAddress(rows_buf));
            int len = *(reinterpret_cast<int*>(rows_buf_ptr));

            // when len = -1, reach the buf's end.
            while (len > 0)
            {
                rows_buf_ptr += 4;
                appendSparkRowToCHColumn(helper, reinterpret_cast<int64_t>(rows_buf_ptr), len);
                rows_buf_ptr += len;
                len = *(reinterpret_cast<int*>(rows_buf_ptr));
            }
            // Try to release reference.
            env->DeleteLocalRef(rows_buf);
        }
        return getWrittenBlock(helper);
    }

    static void freeBlock(Block * block) { delete block; }

private:
    static void appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, int64_t address, int32_t size);
    static Block* getWrittenBlock(SparkRowToCHColumnHelper & helper);
};

class SparkRowReader
{
public:
    bool isSet(int index) const
    {
        assert(index >= 0);
        int64_t mask = 1 << (index & 63);
        int64_t word_offset = base_offset + static_cast<int64_t>(index >> 6) * 8L;
        int64_t word = *reinterpret_cast<int64_t *>(word_offset);
        return (word & mask) != 0;
    }

    void assertIndexIsValid([[maybe_unused]] int index) const
    {
        assert(index >= 0);
        assert(index < num_fields);
    }

    bool isNullAt(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return isSet(ordinal);
    }

    char* getRawDataForFixedNumber(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return reinterpret_cast<char *>(getFieldOffset(ordinal));
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

    explicit SparkRowReader(int32_t numFields) : num_fields(numFields)
    {
        this->bit_set_width_in_bytes = local_engine::calculateBitSetWidthInBytes(numFields);
    }

private:
    int64_t getFieldOffset(int ordinal) const { return base_offset + bit_set_width_in_bytes + ordinal * 8L; }

    int64_t base_offset;
    [[maybe_unused]] int32_t num_fields;
    int32_t size_in_bytes;
    int32_t bit_set_width_in_bytes;
};

}
