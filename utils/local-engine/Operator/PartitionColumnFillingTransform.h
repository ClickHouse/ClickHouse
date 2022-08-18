#pragma once

#include <Processors/ISimpleTransform.h>

namespace local_engine
{
class PartitionColumnFillingTransform : public DB::ISimpleTransform
{
public:
    PartitionColumnFillingTransform(
        const DB::Block & input_,
        const DB::Block & output_,
        const String & partition_col_name_,
        const String & partition_col_value_);
    void transform(DB::Chunk & chunk) override;
    String getName() const override
    {
        return "PartitionColumnFillingTransform";
    }

private:
    DB::ColumnPtr createPartitionColumn();

    DB::DataTypePtr partition_col_type;
    String partition_col_name;
    String partition_col_value;
    DB::ColumnPtr partition_column;
};

}


