#include "DebugUtils.h"
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>

namespace debug
{

void headBlock(const DB::Block & block, size_t count)
{
    std::cerr << "============Block============" << std::endl;
    std::cerr << block.dumpStructure() << std::endl;
    // print header
    for (const auto& name : block.getNames())
    {
        std::cerr << name << "\t";
    }
    std::cerr << std::endl;
    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            const auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;
            auto nested_col = col;
            DB::DataTypePtr nested_type = type;
            if (const auto *nullable = DB::checkAndGetDataType<DB::DataTypeNullable>(type.get()))
            {
                nested_type = nullable->getNestedType();
                const auto *nullable_column = DB::checkAndGetColumn<DB::ColumnNullable>(*col);
                nested_col = nullable_column->getNestedColumnPtr();
            }
            DB::WhichDataType which(nested_type);
            if (col->isNullAt(row))
            {
                std::cerr << "null" << "\t";
            }
            else if (which.isUInt())
            {
                auto value = nested_col->getUInt(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isString())
            {
                auto value = DB::checkAndGetColumn<DB::ColumnString>(*nested_col)->getDataAt(row).toString();
                std::cerr << value << "\t";
            }
            else if (which.isInt())
            {
                auto value = nested_col->getInt(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isFloat32())
            {
                auto value = nested_col->getFloat32(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isFloat64())
            {
                auto value = nested_col->getFloat64(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isDate())
            {
                const auto * date_type = DB::checkAndGetDataType<DB::DataTypeDate>(nested_type.get());
                String date_string;
                DB::WriteBufferFromString wb(date_string);
                date_type->getSerialization(DB::ISerialization::Kind::DEFAULT)->serializeText(*nested_col, row, wb, {});
                std::cerr << date_string.substr(0, 10) << "\t";
            }
            else if (which.isDate32())
            {
                const auto * date_type = DB::checkAndGetDataType<DB::DataTypeDate32>(nested_type.get());
                String date_string;
                DB::WriteBufferFromString wb(date_string);
                date_type->getSerialization(DB::ISerialization::Kind::DEFAULT)->serializeText(*nested_col, row, wb, {});
                std::cerr << date_string.substr(0, 10) << "\t";
            }
            else if (which.isDateTime64())
            {
                const auto * datetime64_type = DB::checkAndGetDataType<DB::DataTypeDateTime64>(nested_type.get());
                String datetime64_string;
                DB::WriteBufferFromString wb(datetime64_string);
                datetime64_type->getSerialization(DB::ISerialization::Kind::DEFAULT)->serializeText(*nested_col, row, wb, {});
                std::cerr << datetime64_string << "\t";
            }
            else
                std::cerr << "N/A" << "\t";
        }
        std::cerr << std::endl;
    }
}

void headColumn(const DB::ColumnPtr column, size_t count)
{
    std::cerr << "============Column============" << std::endl;
    // print header

    std::cerr << column->getName() << "\t";
    std::cerr << std::endl;
    // print rows
    for (size_t row = 0; row < std::min(count, column->size()); ++row)
    {
        auto type = column->getDataType();
        const auto& col = column;
        DB::WhichDataType which(type);
        if (col->isNullAt(row))
        {
            std::cerr << "null" << "\t";
        }
        else if (which.isUInt())
        {
            auto value = col->getUInt(row);
            std::cerr << std::to_string(value) << std::endl;
        }
        else if (which.isString())
        {
            auto value = DB::checkAndGetColumn<DB::ColumnString>(*col)->getDataAt(row).toString();
            std::cerr << value << std::endl;
        }
        else if (which.isInt())
        {
            auto value = col->getInt(row);
            std::cerr << std::to_string(value) << std::endl;
        }
        else if (which.isFloat32())
        {
            auto value = col->getFloat32(row);
            std::cerr << std::to_string(value) << std::endl;
        }
        else if (which.isFloat64())
        {
            auto value = col->getFloat64(row);
            std::cerr << std::to_string(value) << std::endl;
        }
        else
        {
            std::cerr << "N/A" << std::endl;
        }
    }
}
}
