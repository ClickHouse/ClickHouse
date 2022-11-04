#include <gtest/gtest.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SparkRowToCHColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDate.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

using namespace local_engine;
using namespace DB;


struct DataTypeAndField
{
    DataTypePtr type;
    Field field;
};
using DataTypeAndFields = std::vector<DataTypeAndField>;

using SparkRowAndBlock = std::pair<SparkRowInfoPtr, BlockPtr>;

static SparkRowAndBlock mockSparkRowInfoAndBlock(const DataTypeAndFields & type_and_fields)
{
    /// Initialize types
    ColumnsWithTypeAndName columns(type_and_fields.size());
    for (size_t i=0; i<type_and_fields.size(); ++i)
    {
        columns[i].type = type_and_fields[i].type;
        columns[i].name = String(1, 'a' + i);
    }
    Block block(columns);

    /// Initialize columns
    auto mutable_colums = block.mutateColumns();
    for (size_t i=0; i<mutable_colums.size(); ++i)
        mutable_colums[i]->insert(type_and_fields[i].field);
    block.setColumns(std::move(mutable_colums));

    auto converter = CHColumnToSparkRow();
    auto spark_row_info = converter.convertCHColumnToSparkRow(block);
    return std::make_tuple(std::move(spark_row_info), std::make_shared<Block>(std::move(block)));
}

static Int32 getDayNum(const String & date)
{
    ExtendedDayNum res;
    ReadBufferFromString in(date);
    readDateText(res, in);
    return res;
}

static DateTime64 getDateTime64(const String & datetime64, UInt32 scale)
{
    DateTime64 res;
    ReadBufferFromString in(datetime64);
    readDateTime64Text(res, scale, in);
    return res;
}

static void assertReadConsistentWithWritten(const SparkRowInfo & spark_row_info, const Block & in, const DataTypeAndFields type_and_fields)
{
    /// Check if output of SparkRowReader is consistent with types_and_fields
    {
        auto reader = SparkRowReader(spark_row_info.getDataTypes());
        reader.pointTo(spark_row_info.getBufferAddress(), spark_row_info.getTotalBytes());
        for (size_t i = 0; i < type_and_fields.size(); ++i)
        {
            /*
            const auto read_field{std::move(reader.getField(i))};
            const auto & written_field = type_and_fields[i].field;
            std::cout << "read_field:" << read_field.getType() << "," << toString(read_field) << std::endl;
            std::cout << "written_field:" << written_field.getType() << "," << toString(written_field) << std::endl;
            */
            EXPECT_TRUE(reader.getField(i) == type_and_fields[i].field);
        }
    }

    /// check if output of SparkRowToCHColumn is consistents with initial block.
    {
        auto block = SparkRowToCHColumn::convertSparkRowInfoToCHColumn(spark_row_info, in.cloneEmpty());
        const auto & out = *block;
        EXPECT_TRUE(in.rows() == out.rows());
        EXPECT_TRUE(in.columns() == out.columns());
        for (size_t col_idx = 0; col_idx < in.columns(); ++col_idx)
        {
            const auto & in_col = in.getByPosition(col_idx);
            const auto & out_col = out.getByPosition(col_idx);
            for (size_t row_idx = 0; row_idx < in.rows(); ++row_idx)
            {
                const auto in_field = (*in_col.column)[row_idx];
                const auto out_field = (*out_col.column)[row_idx];
                EXPECT_TRUE(in_field == out_field);
            }
        }
    }
}

TEST(SparkRow, BitSetWidthCalculation)
{
    EXPECT_TRUE(calculateBitSetWidthInBytes(0) == 0);
    EXPECT_TRUE(calculateBitSetWidthInBytes(1) == 8);
    EXPECT_TRUE(calculateBitSetWidthInBytes(32) == 8);
    EXPECT_TRUE(calculateBitSetWidthInBytes(64) == 8);
    EXPECT_TRUE(calculateBitSetWidthInBytes(65) == 16);
    EXPECT_TRUE(calculateBitSetWidthInBytes(128) == 16);
}

TEST(SparkRow, GetArrayElementSize)
{
    const std::map<DataTypePtr, int64_t> type_to_size = {
        {std::make_shared<DataTypeInt8>(), 1},
        {std::make_shared<DataTypeUInt8>(), 1},
        {std::make_shared<DataTypeInt16>(), 2},
        {std::make_shared<DataTypeUInt16>(), 2},
        {std::make_shared<DataTypeDate>(), 2},
        {std::make_shared<DataTypeInt32>(), 4},
        {std::make_shared<DataTypeUInt32>(), 4},
        {std::make_shared<DataTypeFloat32>(), 4},
        {std::make_shared<DataTypeDate32>(), 4},
        {std::make_shared<DataTypeDecimal32>(9, 4), 4},
        {std::make_shared<DataTypeInt64>(), 8},
        {std::make_shared<DataTypeUInt64>(), 8},
        {std::make_shared<DataTypeFloat64>(), 8},
        {std::make_shared<DataTypeDateTime64>(6), 8},
        {std::make_shared<DataTypeDecimal64>(18, 4), 8},

        {std::make_shared<DataTypeString>(), 8},
        {std::make_shared<DataTypeDecimal128>(38, 4), 8},
        {std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeString>()), 8},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()), 8},
        {std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeString>()}), 8},
    };

    for (const auto & [type, size] : type_to_size)
    {
        EXPECT_TRUE(BackingDataLengthCalculator::getArrayElementSize(type) == size);
        if (type->canBeInsideNullable())
        {
            const auto type_with_nullable = std::make_shared<DataTypeNullable>(type);
            EXPECT_TRUE(BackingDataLengthCalculator::getArrayElementSize(type_with_nullable) == size);
        }
    }
}

TEST(SparkRow, PrimitiveTypes)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeInt64>(), -1},
        {std::make_shared<DataTypeUInt64>(), UInt64(1)},
        {std::make_shared<DataTypeInt32>(), -2},
        {std::make_shared<DataTypeUInt32>(), UInt32(2)},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + 4 * 8);
}

TEST(SparkRow, PrimitiveStringTypes)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeInt64>(), -1},
        {std::make_shared<DataTypeUInt64>(), UInt64(1)},
        {std::make_shared<DataTypeString>(), "Hello World"},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + (8 * 3) + roundNumberOfBytesToNearestWord(strlen("Hello World")));
}

TEST(SparkRow, PrimitiveStringDateTimestampTypes)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeInt64>(), -1},
        {std::make_shared<DataTypeUInt64>(), UInt64(1)},
        {std::make_shared<DataTypeString>(), "Hello World"},
        {std::make_shared<DataTypeDate32>(), getDayNum("2015-06-22")},
        {std::make_shared<DataTypeDateTime64>(0), getDateTime64("2015-05-08 08:10:25", 0)},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + (8 * 5) + roundNumberOfBytesToNearestWord(strlen("Hello World")));
}


TEST(SparkRow, DecimalTypes)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeDecimal32>(9, 2), DecimalField<Decimal32>(1234, 2)},
        {std::make_shared<DataTypeDecimal64>(18, 2), DecimalField<Decimal64>(5678, 2)},
        {std::make_shared<DataTypeDecimal128>(38, 2), DecimalField<Decimal128>(Decimal128(Int128(12345678)), 2)},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + (8 * 3) + 16);
}


TEST(SparkRow, NullHandling)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt16>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), Null{}},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), Null{}},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + (8 * type_and_fields.size()));
}

TEST(SparkRow, StructTypes)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt32>()}), Tuple{Int32(1)}},
        {std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt64>()})}),
         []() -> Field
         {
             Tuple t(1);
             t.back() = Tuple{Int64(2)};
             return std::move(t);
         }()},
    };

    /*
    for (size_t i=0; i<type_and_fields.size(); ++i)
    {
        std::cerr << "i:" << i << ",field:" << type_and_fields[i].type->getName() << ",field:" << toString(type_and_fields[i].field)
                  << std::endl;
    }
    */

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);

    EXPECT_TRUE(
        spark_row_info->getTotalBytes()
        == 8 + 2 * 8 + BackingDataLengthCalculator(type_and_fields[0].type).calculate(type_and_fields[0].field)
        + BackingDataLengthCalculator(type_and_fields[1].type).calculate(type_and_fields[1].field));
}

TEST(SparkRow, ArrayTypes)
{
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), Array{Int32(1), Int32(2)}},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())),
         []() -> Field
         {
             Array array(1);
             array.back() = Array{Int32(1), Int32(2)};
             return std::move(array);
         }()},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(
        spark_row_info->getTotalBytes()
        == 8 + 2 * 8
            + BackingDataLengthCalculator(type_and_fields[0].type).calculate(type_and_fields[0].field)
            + BackingDataLengthCalculator(type_and_fields[1].type).calculate(type_and_fields[1].field));
}

TEST(SparkRow, MapTypes)
{
    const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>());
    DataTypeAndFields type_and_fields = {
        {map_type,
         []() -> Field
         {
             Map map(2);
             map[0] = std::move(Tuple{Int32(1), Int32(2)});
             map[1] = std::move(Tuple{Int32(3), Int32(4)});
             return std::move(map);
         }()},
        {std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), map_type),
         []() -> Field
         {
             Map inner_map(2);
             inner_map[0] = std::move(Tuple{Int32(5), Int32(6)});
             inner_map[1] = std::move(Tuple{Int32(7), Int32(8)});

             Map map(1);
             map.back() = std::move(Tuple{Int32(9), std::move(inner_map)});
             return std::move(map);
         }()},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);

    EXPECT_TRUE(
        spark_row_info->getTotalBytes()
        == 8 + 2 * 8 + BackingDataLengthCalculator(type_and_fields[0].type).calculate(type_and_fields[0].field)
            + BackingDataLengthCalculator(type_and_fields[1].type).calculate(type_and_fields[1].field));
}


TEST(SparkRow, StructMapTypes)
{
    const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>());
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt64>()});

    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeTuple>(DataTypes{map_type}),
         []() -> Field
         {
             Map map(1);
             map[0] = std::move(Tuple{Int32(1), Int32(2)});
             return std::move(Tuple{std::move(map)});
         }()},
        {std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), tuple_type),
         []() -> Field
         {
             Tuple inner_tuple{Int32(4)};
             Map map(1);
             map.back() = std::move(Tuple{Int32(3), std::move(inner_tuple)});
             return std::move(map);
         }()},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);

    EXPECT_TRUE(
        spark_row_info->getTotalBytes()
        == 8 + 2 * 8 + BackingDataLengthCalculator(type_and_fields[0].type).calculate(type_and_fields[0].field)
            + BackingDataLengthCalculator(type_and_fields[1].type).calculate(type_and_fields[1].field));
}


TEST(SparkRow, StructArrayTypes)
{
    const auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt64>()});
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeTuple>(DataTypes{array_type}),
         []() -> Field
         {
            Array array{Int32(1)};
            Tuple tuple(1);
            tuple[0] = std::move(array);
            return std::move(tuple);
         }()},
        {std::make_shared<DataTypeArray>(tuple_type),
         []() -> Field
         {
            Tuple tuple{Int64(2)};
            Array array(1);
            array[0] = std::move(tuple);
            return std::move(array);
         }()},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(
        spark_row_info->getTotalBytes()
        == 8 + 2 * 8 + BackingDataLengthCalculator(type_and_fields[0].type).calculate(type_and_fields[0].field)
            + BackingDataLengthCalculator(type_and_fields[1].type).calculate(type_and_fields[1].field));

}

TEST(SparkRow, ArrayMapTypes)
{
    const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>());
    const auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());

    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeArray>(map_type),
         []() -> Field
         {
            Map map(1);
            map[0] = std::move(Tuple{Int32(1),Int32(2)});

            Array array(1);
            array[0] = std::move(map);
            return std::move(array);
         }()},
        {std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), array_type),
         []() -> Field
         {
            Array array{Int32(4)};
            Tuple tuple(2);
            tuple[0] = Int32(3);
            tuple[1] = std::move(array);

            Map map(1);
            map[0] = std::move(tuple);
            return std::move(map);
         }()},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);

    EXPECT_TRUE(
        spark_row_info->getTotalBytes()
        == 8 + 2 * 8 + BackingDataLengthCalculator(type_and_fields[0].type).calculate(type_and_fields[0].field)
            + BackingDataLengthCalculator(type_and_fields[1].type).calculate(type_and_fields[1].field));
}


TEST(SparkRow, NullableComplexTypes)
{
    const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>());
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt64>()});
    const auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
    DataTypeAndFields type_and_fields = {
        {std::make_shared<DataTypeNullable>(map_type), Null{}},
        {std::make_shared<DataTypeNullable>(tuple_type), Null{}},
        {std::make_shared<DataTypeNullable>(array_type), Null{}},
    };

    SparkRowInfoPtr spark_row_info;
    BlockPtr block;
    std::tie(spark_row_info, block) = mockSparkRowInfoAndBlock(type_and_fields);
    assertReadConsistentWithWritten(*spark_row_info, *block, type_and_fields);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + 3 * 8);
}

