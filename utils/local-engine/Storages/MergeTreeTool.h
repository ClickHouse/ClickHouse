#pragma once
#include<Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

using namespace DB;

namespace local_engine
{
    std::shared_ptr<DB::StorageInMemoryMetadata> buildMetaData(DB::NamesAndTypesList& columns)
    {
        std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
        ColumnsDescription columns_description;
        for (const auto &item : columns)
        {
            columns_description.add(ColumnDescription(item.name, item.type))
        }
        metadata->setColumns(columns_description);
        metadata->partition_key.expression_list_ast = std::make_shared<ASTExpressionList>();
        metadata->sorting_key = KeyDescription::getSortingKeyFromAST(makeASTFunction("tuple"), columns_description, global_context, {});
        metadata->primary_key.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>());
        return metadata;
    }

    std::unique_ptr<MergeTreeSettings> buildMergeTreeSettings()
    {
        auto settings = std::make_unique<DB::MergeTreeSettings>();
        settings->set("min_bytes_for_wide_part", Field(0));
        settings->set("min_rows_for_wide_part", Field(0));
        return settings;
    }

}
