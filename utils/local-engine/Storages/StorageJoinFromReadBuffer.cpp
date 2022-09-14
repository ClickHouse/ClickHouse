#include "StorageJoinFromReadBuffer.h"

#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <QueryPipeline/Pipe.h>
#include <Common/Exception.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/joinDispatch.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
}

template <typename T>
static const char * rawData(T & t)
{
    return reinterpret_cast<const char *>(&t);
}
template <typename T>
static size_t rawSize(T &)
{
    return sizeof(T);
}
template <>
const char * rawData(const StringRef & t)
{
    return t.data;
}
template <>
size_t rawSize(const StringRef & t)
{
    return t.size;
}

class JoinSource : public SourceWithProgress
{
public:
    JoinSource(HashJoinPtr join_, TableLockHolder lock_holder_, UInt64 max_block_size_, Block sample_block_)
        : SourceWithProgress(sample_block_)
        , join(join_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
    {
        if (!join->getTableJoin().oneDisjunct())
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin does not support OR for keys in JOIN ON section");

        column_indices.resize(sample_block.columns());

        auto & saved_block = join->getJoinedData()->sample_block;

        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            auto & [_, type, name] = sample_block.getByPosition(i);
            if (join->right_table_keys.has(name))
            {
                key_pos = i;
                const auto & column = join->right_table_keys.getByName(name);
                restored_block.insert(column);
            }
            else
            {
                size_t pos = saved_block.getPositionByName(name);
                column_indices[i] = pos;

                const auto & column = saved_block.getByPosition(pos);
                restored_block.insert(column);
            }
        }
    }

    String getName() const override { return "Join"; }

protected:
    Chunk generate() override
    {
        if (join->data->blocks.empty())
            return {};

        Chunk chunk;
        if (!joinDispatch(join->kind, join->strictness, join->data->maps.front(),
                          [&](auto kind, auto strictness, auto & map) { chunk = createChunk<kind, strictness>(map); }))
            throw Exception("Logical error: unknown JOIN strictness", ErrorCodes::LOGICAL_ERROR);
        return chunk;
    }

private:
    HashJoinPtr join;
    TableLockHolder lock_holder;

    UInt64 max_block_size;
    Block sample_block;
    Block restored_block; /// sample_block with parent column types

    ColumnNumbers column_indices;
    std::optional<size_t> key_pos;

    std::unique_ptr<void, std::function<void(void *)>> position; /// type erasure

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Chunk createChunk(const Maps & maps)
    {
        MutableColumns mut_columns = restored_block.cloneEmpty().mutateColumns();

        size_t rows_added = 0;

        switch (join->data->type)
        {
#define M(TYPE)                                           \
    case HashJoin::Type::TYPE:                                \
        rows_added = fillColumns<KIND, STRICTNESS>(*maps.TYPE, mut_columns); \
        break;
            APPLY_FOR_JOIN_VARIANTS_LIMITED(M)
#undef M

            default:
                throw Exception("Unsupported JOIN keys in StorageJoin. Type: " + toString(static_cast<UInt32>(join->data->type)),
                                ErrorCodes::UNSUPPORTED_JOIN_KEYS);
        }

        if (!rows_added)
            return {};

        Columns columns;
        columns.reserve(mut_columns.size());
        for (auto & col : mut_columns)
            columns.emplace_back(std::move(col));

        /// Correct nullability and LowCardinality types
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & src = restored_block.getByPosition(i);
            const auto & dst = sample_block.getByPosition(i);

            if (!src.type->equals(*dst.type))
            {
                auto arg = src;
                arg.column = std::move(columns[i]);
                columns[i] = castColumn(arg, dst.type);
            }
        }

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns)
    {
        size_t rows_added = 0;

        if (!position)
            position = decltype(position)(
                static_cast<void *>(new typename Map::const_iterator(map.begin())), //-V572
                [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

        auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
        auto end = map.end();

        for (; it != end; ++it)
        {
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::RightAny)
            {
                fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
            {
                fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                if constexpr (KIND == ASTTableJoin::Kind::Left || KIND == ASTTableJoin::Kind::Inner)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == ASTTableJoin::Kind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Semi)
            {
                if constexpr (KIND == ASTTableJoin::Kind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == ASTTableJoin::Kind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Anti)
            {
                if constexpr (KIND == ASTTableJoin::Kind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == ASTTableJoin::Kind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else
                throw Exception("This JOIN is not implemented yet", ErrorCodes::NOT_IMPLEMENTED);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }

    template <typename Map>
    static void fillOne(MutableColumns & columns, const ColumnNumbers & column_indices, typename Map::const_iterator & it,
                        const std::optional<size_t> & key_pos, size_t & rows_added)
    {
        for (size_t j = 0; j < columns.size(); ++j)
            if (j == key_pos)
                columns[j]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
            else
                columns[j]->insertFrom(*it->getMapped().block->getByPosition(column_indices[j]).column.get(), it->getMapped().row_num);
        ++rows_added;
    }

    template <typename Map>
    static void fillAll(MutableColumns & columns, const ColumnNumbers & column_indices, typename Map::const_iterator & it,
                        const std::optional<size_t> & key_pos, size_t & rows_added)
    {
        for (auto ref_it = it->getMapped().begin(); ref_it.ok(); ++ref_it)
        {
            for (size_t j = 0; j < columns.size(); ++j)
                if (j == key_pos)
                    columns[j]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
                else
                    columns[j]->insertFrom(*ref_it->block->getByPosition(column_indices[j]).column.get(), ref_it->row_num);
            ++rows_added;
        }
    }
};

}

using namespace DB;

namespace local_engine
{

void StorageJoinFromReadBuffer::rename(const String &  /*new_path_to_table_data*/, const DB::StorageID &  /*new_table_id*/)
{
    throw std::runtime_error("unsupported operation");
}
DB::SinkToStoragePtr
StorageJoinFromReadBuffer::write(const DB::ASTPtr &  /*query*/, const DB::StorageMetadataPtr &  /*ptr*/, DB::ContextPtr  /*context*/)
{
    throw std::runtime_error("unsupported operation");
}
bool StorageJoinFromReadBuffer::storesDataOnDisk() const
{
    return false;
}
DB::Strings StorageJoinFromReadBuffer::getDataPaths() const
{
    throw std::runtime_error("unsupported operation");
}

void StorageJoinFromReadBuffer::finishInsert()
{
    in.reset();
}

DB::Pipe StorageJoinFromReadBuffer::read(
    const DB::Names & column_names,
    const DB::StorageSnapshotPtr & storage_snapshot,
    DB::SelectQueryInfo &  /*query_info*/,
    DB::ContextPtr context,
    DB::QueryProcessingStage::Enum  /*processed_stage*/,
    size_t max_block_size,
    unsigned int  /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block source_sample_block = storage_snapshot->getSampleBlockForColumns(column_names);
    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return Pipe(std::make_shared<JoinSource>(join, std::move(holder), max_block_size, source_sample_block));
}

void StorageJoinFromReadBuffer::restore()
{
    if (!in)
    {
        throw std::runtime_error("input reader buffer is not available");
    }
    ContextPtr ctx = nullptr;
    NativeReader block_stream(*in, 0);

    ProfileInfo info;
    while (Block block = block_stream.read())
    {
        auto final_block = sample_block.cloneWithColumns(block.mutateColumns());
        info.update(final_block);
        insertBlock(final_block, ctx);
    }

    finishInsert();
}
void StorageJoinFromReadBuffer::insertBlock(const Block & block, DB::ContextPtr context)
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);
    join->addJoinedBlock(block, true);
}
size_t StorageJoinFromReadBuffer::getSize(DB::ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return join->getTotalRowCount();
}
DB::RWLockImpl::LockHolder
StorageJoinFromReadBuffer::tryLockTimedWithContext(const RWLock & lock, DB::RWLockImpl::Type type, DB::ContextPtr context) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef().lock_acquire_timeout : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return tryLockTimed(lock, type, query_id, acquire_timeout);
}
StorageJoinFromReadBuffer::StorageJoinFromReadBuffer(
    std::unique_ptr<DB::ReadBuffer> in_,
    const StorageID & table_id_,
    const Names & key_names_,
    bool use_nulls_,
    DB::SizeLimits limits_,
    DB::ASTTableJoin::Kind kind_,
    DB::ASTTableJoin::Strictness strictness_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const bool overwrite_,
    const String & relative_path_) : StorageSetOrJoinBase{nullptr, relative_path_, table_id_, columns_, constraints_, comment, false}
        , key_names(key_names_)
        , use_nulls(use_nulls_)
        , limits(limits_)
        , kind(kind_)
        , strictness(strictness_)
        , overwrite(overwrite_)
        , in(std::move(in_))
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    sample_block = metadata_snapshot->getSampleBlock();
    for (const auto & key : key_names)
        if (!metadata_snapshot->getColumns().hasPhysical(key))
            throw Exception{"Key column (" + key + ") does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};

    table_join = std::make_shared<TableJoin>(limits, use_nulls, kind, strictness, key_names);
    join = std::make_shared<HashJoin>(table_join, getRightSampleBlock(), overwrite);
    restore();
}
DB::HashJoinPtr StorageJoinFromReadBuffer::getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr context) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (!analyzed_join->sameStrictnessAndKind(strictness, kind))
        throw Exception("Table " + getStorageID().getNameForLogs() + " has incompatible type of JOIN.", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);

    if ((analyzed_join->forceNullableRight() && !use_nulls) ||
        (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Table {} needs the same join_use_nulls setting as present in LEFT or FULL JOIN",
            getStorageID().getNameForLogs());

    /// TODO: check key columns

    /// Set names qualifiers: table.column -> column
    /// It's required because storage join stores non-qualified names
    /// Qualifies will be added by join implementation (HashJoin)
    analyzed_join->setRightKeys(key_names);

    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, getRightSampleBlock());

    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    join_clone->setLock(holder);
    join_clone->reuseJoinedData(*join);

    return join_clone;
}
}
