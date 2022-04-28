#pragma once
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>

namespace debug
{

void headBlock(const DB::Block & block, size_t count=10)
{
    std::cerr << "============Block============" << std::endl;
    // print header
    for (auto name : block.getNames())
    {
        std::cerr << name << "\t";
    }
    std::cerr << std::endl;
    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;
            DB::WhichDataType which(type);
            if (which.isUInt())
            {
                auto value = DB::checkAndGetColumn<DB::ColumnUInt64>(*col)->getUInt(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isString())
            {
                auto value = DB::checkAndGetColumn<DB::ColumnString>(*col)->getDataAt(row).toString();
                std::cerr << value << "\t";
            }
            else if (which.isInt())
            {
                auto value = col->getInt(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isFloat32())
            {
                auto value = col->getFloat32(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else if (which.isFloat64())
            {
                auto value = col->getFloat64(row);
                std::cerr << std::to_string(value) << "\t";
            }
            else
            {
                std::cerr << "N/A"
                          << "\t";
            }
        }
        std::cerr << std::endl;
    }
}

void headColumn(const DB::ColumnPtr column, size_t count=10)
{
    std::cerr << "============Column============" << std::endl;
    // print header

    std::cerr << column->getName() << "\t";
    std::cerr << std::endl;
    // print rows
    for (size_t row = 0; row < std::min(count, column->size()); ++row)
    {
        auto type = column->getDataType();
        auto col = column;
        DB::WhichDataType which(type);
        if (which.isUInt())
        {
            auto value = DB::checkAndGetColumn<DB::ColumnUInt64>(*col)->getUInt(row);
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
            std::cerr << "N/A"
                      << std::endl;
        }
    }
}
}
