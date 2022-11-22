#pragma once
#include <Core/ColumnWithTypeAndName.h>
#include <Functions/IFunction.h>
#include <Processors/Chunk.h>
#include <base/types.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>
#include <Common/BlockIterator.h>
#include <memory>
#include <vector>
#include <substrait/plan.pb.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
namespace local_engine
{
class RoundRobinSelectorBuilder
{
public:
    explicit RoundRobinSelectorBuilder(size_t parts_num_) : parts_num(parts_num_) {}
    std::vector<DB::IColumn::ColumnIndex> build(DB::Block & block);
private:
    size_t parts_num;
    Int32 pid_selection = 0;
};

class HashSelectorBuilder
{
public:
    explicit HashSelectorBuilder(UInt32 parts_num_, const std::vector<std::string> & exprs_, const std::string & hash_function_name_);
    std::vector<DB::IColumn::ColumnIndex> build(DB::Block & block);
private:
    UInt32 parts_num;
    std::vector<std::string> exprs;
    std::string hash_function_name;
    DB::FunctionBasePtr hash_function;
};

class RangeSelectorBuilder
{
public:
    explicit RangeSelectorBuilder(const std::string & options_);
    std::vector<DB::IColumn::ColumnIndex> build(DB::Block & block);
private:
    DB::SortDescription sort_descriptions;
    std::vector<size_t> sorting_key_columns;
    struct SortFieldTypeInfo
    {
        DB::DataTypePtr inner_type;
        bool is_nullable = false;
    };
    std::vector<SortFieldTypeInfo> sort_field_types;
    DB::Block range_bounds_block;

    // If the ordering keys have expressions, we caculate the expressions here.
    std::mutex actions_dag_mutex;
    std::unique_ptr<substrait::Plan> projection_plan_pb;
    std::atomic<bool> has_init_actions_dag;
    std::unique_ptr<DB::ExpressionActions> projection_expression_actions;

    void initSortInformation(Poco::JSON::Array::Ptr orderings);
    void initRangeBlock(Poco::JSON::Array::Ptr range_bounds);
    void initActionsDAG(const DB::Block & block);

    void computePartitionIdByBinarySearch(DB::Block & block, std::vector<DB::IColumn::ColumnIndex> & selector);
    int compareRow(
        const DB::Columns & columns,
        const std::vector<size_t> & required_columns,
        size_t row,
        const DB::Columns & bound_columns,
        size_t bound_row);

    int binarySearchBound(
        const DB::Columns & bound_columns,
        Int64 l,
        Int64 r,
        const DB::Columns & columns,
        const std::vector<size_t> & used_cols,
        size_t row);
};

}
