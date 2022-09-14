#pragma once
#include <base/shared_ptr_helper.h>

#include <Interpreters/join_common.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/StorageSet.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/TableLockHolder.h>
#include <Common/RWLock.h>
#include <Storages/StorageJoin.h>

namespace local_engine
{
class StorageJoinFromReadBuffer : public shared_ptr_helper<StorageJoinFromReadBuffer>, public DB::StorageSetOrJoinBase
{
    friend struct shared_ptr_helper<StorageJoinFromReadBuffer>;

public:
    StorageJoinFromReadBuffer(
        std::unique_ptr<DB::ReadBuffer> in_,
        const DB::StorageID & table_id_,
        const DB::Names & key_names_,
        bool use_nulls_,
        DB::SizeLimits limits_,
        DB::ASTTableJoin::Kind kind_,
        DB::ASTTableJoin::Strictness strictness_,
        const DB::ColumnsDescription & columns_,
        const DB::ConstraintsDescription & constraints_,
        const String & comment,
        bool overwrite_,
        const String & relative_path_ = "/tmp" /* useless variable */);

    String getName() const override { return "Join"; }

    void rename(const String & new_path_to_table_data, const DB::StorageID & new_table_id) override;
    DB::HashJoinPtr getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr context) const;
    DB::SinkToStoragePtr write(const DB::ASTPtr & query, const DB::StorageMetadataPtr & ptr, DB::ContextPtr context) override;
    bool storesDataOnDisk() const override;
    DB::Strings getDataPaths() const override;
    DB::Pipe read(
        const DB::Names & column_names,
        const DB::StorageSnapshotPtr & storage_snapshot,
        DB::SelectQueryInfo & query_info,
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;
    DB::Block getRightSampleBlock() const
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();
        DB::Block block = metadata_snapshot->getSampleBlock();
        if (use_nulls && isLeftOrFull(kind))
        {
            for (auto & col : block)
            {
                DB::JoinCommon::convertColumnToNullable(col);
            }
        }
        return block;
    }
protected:
    void restore();

private:
    void insertBlock(const DB::Block & block, DB::ContextPtr context) override;
    void finishInsert() override;
    size_t getSize(DB::ContextPtr context) const override;
    DB::RWLockImpl::LockHolder tryLockTimedWithContext(const DB::RWLock & lock, DB::RWLockImpl::Type type, DB::ContextPtr context) const;

    DB::Block sample_block;
    const DB::Names key_names;
    bool use_nulls;
    DB::SizeLimits limits;
    DB::ASTTableJoin::Kind kind;                    /// LEFT | INNER ...
    DB::ASTTableJoin::Strictness strictness;        /// ANY | ALL
    bool overwrite;

    std::shared_ptr<DB::TableJoin> table_join;
    DB::HashJoinPtr join;

    std::unique_ptr<DB::ReadBuffer> in;

    /// Protect state for concurrent use in insertFromBlock and joinBlock.
    /// Lock is stored in HashJoin instance during query and blocks concurrent insertions.
    mutable DB::RWLock rwlock = DB::RWLockImpl::create();
    mutable std::mutex mutate_mutex;
};
}


