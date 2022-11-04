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
        std::cerr << name << "\t";
    std::cerr << std::endl;

    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            const auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;

            if (column > 0)
                std::cerr << "\t";
            std::cerr << toString((*col)[row]);
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
        std::cerr << toString((*column)[row]) << std::endl;
}

}
