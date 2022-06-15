#pragma once
#include <Shuffle/ShuffleReader.h>
#include <Storages/StorageJoinFromReadBuffer.h>

namespace local_engine
{
class BroadCastJoinBuilder
{
public:
    static void buildJoinIfNotExist(
        const std::string & key,
        std::unique_ptr<ReadBufferFromJavaInputStream> read_buffer,
        const DB::Names & key_names_,
        DB::ASTTableJoin::Kind kind_,
        DB::ASTTableJoin::Strictness strictness_,
        const DB::ColumnsDescription & columns_);

    static void buildJoinIfNotExist(
        const std::string & key,
        std::unique_ptr<ReadBufferFromJavaInputStream> read_buffer,
        const std::string & join_keys,
        const std::string & join_type,
        const std::string & named_struct);

    static std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key);

private:
    static std::unordered_map<std::string, std::shared_ptr<StorageJoinFromReadBuffer>> storage_join_map;
    static std::unordered_map<std::string, std::shared_ptr<std::mutex>> storage_join_lock;
    static std::mutex join_lock_mutex;
};
}
