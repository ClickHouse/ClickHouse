#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MutationCommands.h>
#include <base/shared_ptr_helper.h>

namespace local_engine
{
using namespace DB;

class CustomMergeTreeSink;

class CustomStorageMergeTree final : public shared_ptr_helper<CustomStorageMergeTree>, public MergeTreeData
{
    friend struct shared_ptr_helper<CustomStorageMergeTree>;
    friend class CustomMergeTreeSink;

public:
    CustomStorageMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag = false);
    std::string getName() const override;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & executor) override;

    MergeTreeDataWriter writer;
    MergeTreeDataSelectExecutor reader;

private:
    SimpleIncrement increment;

    void startBackgroundMovesIfNeeded() override;
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

protected:
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    PartitionCommandsResultInfo
    attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;
};

}
