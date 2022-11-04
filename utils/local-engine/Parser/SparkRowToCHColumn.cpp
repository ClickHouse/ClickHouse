#include "SparkRowToCHColumn.h"
#include <memory>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Common/Exception.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
}
}

using namespace DB;

namespace local_engine
{
jclass SparkRowToCHColumn::spark_row_interator_class = nullptr;
jmethodID SparkRowToCHColumn::spark_row_interator_hasNext = nullptr;
jmethodID SparkRowToCHColumn::spark_row_interator_next = nullptr;
jmethodID SparkRowToCHColumn::spark_row_iterator_nextBatch = nullptr;

ALWAYS_INLINE static void writeRowToColumns(std::vector<MutableColumnPtr> & columns, const SparkRowReader & spark_row_reader)
{
    auto num_fields = columns.size();
    const auto & field_types = spark_row_reader.getFieldTypes();
    for (size_t i = 0; i < num_fields; i++)
    {
        if (spark_row_reader.supportRawData(i))
        {
            const StringRef str{std::move(spark_row_reader.getStringRef(i))};
            columns[i]->insertData(str != EMPTY_STRING_REF ? str.data : nullptr, str.size);
        }
        else
            columns[i]->insert(spark_row_reader.getField(i));
    }
}

std::unique_ptr<Block>
SparkRowToCHColumn::convertSparkRowInfoToCHColumn(const SparkRowInfo & spark_row_info, const Block & header)
{
    auto block = std::make_unique<Block>();
    *block = std::move(header.cloneEmpty());
    MutableColumns mutable_columns{std::move(block->mutateColumns())};
    const auto num_rows = spark_row_info.getNumRows();
    for (size_t col_i = 0; col_i < header.columns(); ++col_i)
        mutable_columns[col_i]->reserve(num_rows);

    DataTypes types{std::move(header.getDataTypes())};
    SparkRowReader row_reader(types);
    for (int64_t i = 0; i < num_rows; i++)
    {
        row_reader.pointTo(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i], spark_row_info.getLengths()[i]);
        writeRowToColumns(mutable_columns, row_reader);
    }
    block->setColumns(std::move(mutable_columns));
    return std::move(block);
}

void SparkRowToCHColumn::appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, char * buffer, int32_t length)
{
    SparkRowReader row_reader(helper.data_types);
    row_reader.pointTo(buffer, length);
    writeRowToColumns(helper.mutable_columns, row_reader);
    ++helper.rows;
}

Block * SparkRowToCHColumn::getBlock(SparkRowToCHColumnHelper & helper)
{
    auto * block = new Block();
    if (helper.header.columns())
    {
        *block = std::move(helper.header.cloneEmpty());
        block->setColumns(std::move(helper.mutable_columns));
    }
    else
    {
        // In some cases, there is no required columns in spark plan, E.g. count(*).
        // In these cases, the rows is the only needed information, so we try to create
        // a block with a const column which will not be really used any where.
        auto uint8_ty = std::make_shared<DB::DataTypeUInt8>();
        auto col = uint8_ty->createColumnConst(helper.rows, 0);
        ColumnWithTypeAndName named_col(col, uint8_ty, "__anonymous_col__");
        block->insert(named_col);
    }
    return block;
}

VariableLengthDataReader::VariableLengthDataReader(const DataTypePtr & type_)
    : type(type_), type_without_nullable(removeNullable(type)), which(type_without_nullable)
{
    if (!BackingDataLengthCalculator::isVariableLengthDataType(type_without_nullable))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", type->getName());
}

Field VariableLengthDataReader::read(const char *buffer, size_t length) const
{
    if (which.isStringOrFixedString())
        return std::move(readString(buffer, length));

    if (which.isDecimal128())
        return std::move(readDecimal(buffer, length));

    if (which.isArray())
        return std::move(readArray(buffer, length));

    if (which.isMap())
        return std::move(readMap(buffer, length));

    if (which.isTuple())
        return std::move(readStruct(buffer, length));

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", type->getName());
}

StringRef VariableLengthDataReader::readUnalignedBytes(const char * buffer, size_t length) const
{
    return {buffer, length};
}

Field VariableLengthDataReader::readDecimal(const char * buffer, size_t length) const
{
    assert(sizeof(Decimal128) == length);

    Decimal128 value;
    memcpy(&value, buffer, length);

    const auto * decimal128_type = typeid_cast<const DataTypeDecimal128 *>(type_without_nullable.get());
    return std::move(DecimalField<Decimal128>(value, decimal128_type->getScale()));
}

Field VariableLengthDataReader::readString(const char * buffer, size_t length) const
{
    String str(buffer, length);
    return std::move(Field(std::move(str)));
}

Field VariableLengthDataReader::readArray(const char * buffer, [[maybe_unused]] size_t length) const
{
    /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing data
    /// Read numElements
    int64_t num_elems = 0;
    memcpy(&num_elems, buffer, 8);
    if (num_elems == 0)
        return Array();

    /// Skip null_bitmap
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_elems);

    /// Read values
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    const auto & nested_type = array_type->getNestedType();
    const auto elem_size = BackingDataLengthCalculator::getArrayElementSize(nested_type);
    const auto len_values = roundNumberOfBytesToNearestWord(elem_size * num_elems);
    Array array;
    array.reserve(num_elems);

    if (BackingDataLengthCalculator::isFixedLengthDataType(removeNullable(nested_type)))
    {
        FixedLengthDataReader reader(nested_type);
        for (int64_t i = 0; i < num_elems; ++i)
        {
            if (isBitSet(buffer + 8, i))
            {
                array.emplace_back(std::move(Null{}));
            }
            else
            {
                const auto elem = reader.read(buffer + 8 + len_null_bitmap + i * elem_size);
                array.emplace_back(elem);
            }
        }
    }
    else if (BackingDataLengthCalculator::isVariableLengthDataType(removeNullable(nested_type)))
    {
        VariableLengthDataReader reader(nested_type);
        for (int64_t i = 0; i < num_elems; ++i)
        {
            if (isBitSet(buffer + 8, i))
            {
                array.emplace_back(std::move(Null{}));
            }
            else
            {
                int64_t offset_and_size = 0;
                memcpy(&offset_and_size, buffer + 8 + len_null_bitmap + i * 8, 8);
                const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
                const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);

                const auto elem = reader.read(buffer + offset, size);
                array.emplace_back(elem);
            }
        }
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", nested_type->getName());

    return std::move(array);
}

Field VariableLengthDataReader::readMap(const char * buffer, size_t length) const
{
    /// 内存布局：Length of UnsafeArrayData of key(8B) |  UnsafeArrayData of key | UnsafeArrayData of value
    /// Read Length of UnsafeArrayData of key
    int64_t key_array_size = 0;
    memcpy(&key_array_size, buffer, 8);
    if (key_array_size == 0)
        return std::move(Map());

    /// Read UnsafeArrayData of keys
    const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
    const auto & key_type = map_type->getKeyType();
    const auto key_array_type = std::make_shared<DataTypeArray>(key_type);
    VariableLengthDataReader key_reader(key_array_type);
    auto key_field = key_reader.read(buffer + 8, key_array_size);
    auto & key_array = key_field.safeGet<Array>();

    /// Read UnsafeArrayData of values
    const auto & val_type = map_type->getValueType();
    const auto val_array_type = std::make_shared<DataTypeArray>(val_type);
    VariableLengthDataReader val_reader(val_array_type);
    auto val_field = val_reader.read(buffer + 8 + key_array_size, length - 8 - key_array_size);
    auto & val_array = val_field.safeGet<Array>();

    /// Construct map in CH way [(k1, v1), (k2, v2), ...]
    if (key_array.size() != val_array.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key size {} not equal to value size {} in map", key_array.size(), val_array.size());
    Map map(key_array.size());
    for (size_t i = 0; i < key_array.size(); ++i)
    {
        Tuple tuple(2);
        tuple[0] = std::move(key_array[i]);
        tuple[1] = std::move(val_array[i]);

        map[i] = std::move(tuple);
    }
    return std::move(map);
}

Field VariableLengthDataReader::readStruct(const char * buffer, size_t  /*length*/) const
{
    /// 内存布局：null_bitmap(字节数与字段数成正比) | values(num_fields * 8B) | backing data
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
    const auto & field_types = tuple_type->getElements();
    const auto num_fields = field_types.size();
    if (num_fields == 0)
        return std::move(Tuple());

    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_fields);

    Tuple tuple(num_fields);
    for (size_t i=0; i<num_fields; ++i)
    {
        const auto & field_type = field_types[i];
        if (isBitSet(buffer, i))
        {
            tuple[i] = std::move(Null{});
            continue;
        }

        if (BackingDataLengthCalculator::isFixedLengthDataType(removeNullable(field_type)))
        {
            FixedLengthDataReader reader(field_type);
            tuple[i] = std::move(reader.read(buffer + len_null_bitmap + i * 8));
        }
        else if (BackingDataLengthCalculator::isVariableLengthDataType(removeNullable(field_type)))
        {
            int64_t offset_and_size = 0;
            memcpy(&offset_and_size, buffer + len_null_bitmap + i * 8, 8);
            const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
            const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);

            VariableLengthDataReader reader(field_type);
            tuple[i] = std::move(reader.read(buffer + offset, size));
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", field_type->getName());
    }
    return std::move(tuple);
}

FixedLengthDataReader::FixedLengthDataReader(const DataTypePtr & type_)
    : type(type_), type_without_nullable(removeNullable(type)), which(type_without_nullable)
{
    if (!BackingDataLengthCalculator::isFixedLengthDataType(type_without_nullable) || !type_without_nullable->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", type->getName());

    value_size = type_without_nullable->getSizeOfValueInMemory();
}

StringRef FixedLengthDataReader::unsafeRead(const char * buffer) const
{
    return {buffer, value_size};
}

Field FixedLengthDataReader::read(const char * buffer) const
{
    if (which.isUInt8())
    {
        UInt8 value = 0;
        memcpy(&value, buffer, 1);
        return value;
    }

    if (which.isUInt16() || which.isDate())
    {
        UInt16 value = 0;
        memcpy(&value, buffer, 2);
        return value;
    }

    if (which.isUInt32())
    {
        UInt32 value = 0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isUInt64())
    {
        UInt64 value = 0;
        memcpy(&value, buffer, 8);
        return value;
    }

    if (which.isInt8())
    {
        Int8 value = 0;
        memcpy(&value, buffer, 1);
        return value;
    }

    if (which.isInt16())
    {
        Int16 value = 0;
        memcpy(&value, buffer, 2);
        return value;
    }

    if (which.isInt32() || which.isDate32())
    {
        Int32 value = 0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isInt64())
    {
        Int64 value = 0;
        memcpy(&value, buffer, 8);
        return value;
    }

    if (which.isFloat32())
    {
        Float32 value = 0.0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isFloat64())
    {
        Float64 value = 0.0;
        memcpy(&value, buffer, 8);
        return value;
    }

    if (which.isDecimal32())
    {
        Decimal32 value = 0;
        memcpy(&value, buffer, 4);

        const auto * decimal32_type = typeid_cast<const DataTypeDecimal32 *>(type_without_nullable.get());
        return std::move(DecimalField{value, decimal32_type->getScale()});
    }

    if (which.isDecimal64() || which.isDateTime64())
    {
        Decimal64 value = 0;
        memcpy(&value, buffer, 8);

        UInt32 scale = which.isDecimal64() ? typeid_cast<const DataTypeDecimal64 *>(type_without_nullable.get())->getScale()
                                           : typeid_cast<const DataTypeDateTime64 *>(type_without_nullable.get())->getScale();
        return std::move(DecimalField{value, scale});
    }
    throw Exception(ErrorCodes::UNKNOWN_TYPE, "FixedLengthDataReader doesn't support type {}", type->getName());
}

}
