#pragma once

#include <Core/Block.h>

namespace debug
{

void headBlock(const DB::Block & block, size_t count=10);

void headColumn(const DB::ColumnPtr column, size_t count=10);
}
