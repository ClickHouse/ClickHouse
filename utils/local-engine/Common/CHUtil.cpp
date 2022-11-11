#include <filesystem>
#include <Common/CHUtil.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}
namespace local_engine
{
constexpr auto VIRTUAL_ROW_COUNT_COLOUMN = "__VIRTUAL_ROW_COUNT_COLOUMNOUMN__";

namespace fs = std::filesystem;

DB::Block BlockUtil::buildRowCountHeader()
{
    DB::Block header;
    auto uint8_ty = std::make_shared<DB::DataTypeUInt8>();
    auto col = uint8_ty->createColumn();
    DB::ColumnWithTypeAndName named_col(std::move(col), uint8_ty, VIRTUAL_ROW_COUNT_COLOUMN);
    header.insert(named_col);
    return header.cloneEmpty();
}

DB::Chunk BlockUtil::buildRowCountChunk(UInt64 rows)
{
    auto data_type = std::make_shared<DB::DataTypeUInt8>();
    auto col = data_type->createColumnConst(rows, 0);
    DB::Columns res_columns;
    res_columns.emplace_back(std::move(col));
    return DB::Chunk(std::move(res_columns), rows);
}

DB::Block BlockUtil::buildRowCountBlock(UInt64 rows)
{
    DB::Block block;
    auto uint8_ty = std::make_shared<DB::DataTypeUInt8>();
    auto col = uint8_ty->createColumnConst(rows, 0);
    DB::ColumnWithTypeAndName named_col(col, uint8_ty, VIRTUAL_ROW_COUNT_COLOUMN);
    block.insert(named_col);
    return block;
}

DB::Block BlockUtil::buildHeader(const DB::NamesAndTypesList & names_types_list)
{
    DB::ColumnsWithTypeAndName cols;
    for (const auto & name_type : names_types_list)
    {
        DB::ColumnWithTypeAndName col(name_type.type->createColumn(), name_type.type, name_type.name);
        cols.emplace_back(col);
    }
    return DB::Block(cols);
}


std::string PlanUtil::explainPlan(DB::QueryPlan & plan)
{
    std::string plan_str;
    DB::QueryPlan::ExplainPlanOptions buf_opt
    {
        .header = true,
        .actions = true,
        .indexes = true,
    };
    DB::WriteBufferFromOwnString buf;
    plan.explainPlan(buf, buf_opt);
    plan_str = buf.str();
    return plan_str;
}

std::vector<MergeTreeUtil::Path> MergeTreeUtil::getAllMergeTreeParts(const Path &storage_path)
{
    if (!fs::exists(storage_path))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid merge tree store path:{}", storage_path.string());
    }

    // TODO: May need to check the storage format version
    std::vector<fs::path> res;
    for (const auto & entry : fs::directory_iterator(storage_path))
    {
        auto filename = entry.path().filename();
        if (filename == "format_version.txt" || filename == "detached")
            continue;
        res.push_back(entry.path());
    }
    return res;
}

DB::NamesAndTypesList MergeTreeUtil::getSchemaFromMergeTreePart(const fs::path & part_path)
{
    DB::NamesAndTypesList names_types_list;
    if (!fs::exists(part_path))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid merge tree store path:{}", part_path.string());
    }
    DB::ReadBufferFromFile readbuffer((part_path / "columns.txt").string());
    names_types_list.readText(readbuffer);
    return names_types_list;
}

}
