#include "BroadCastJoinBuilder.h"
#include <Parser/SerializedPlanParser.h>
#include <Poco/StringTokenizer.h>
#include <Common/ThreadPool.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
using namespace DB;

std::queue<std::string> BroadCastJoinBuilder::storage_join_queue;
std::unordered_map<std::string, std::shared_ptr<StorageJoinFromReadBuffer>> BroadCastJoinBuilder::storage_join_map;
std::unordered_map<std::string, std::shared_ptr<std::mutex>> BroadCastJoinBuilder::storage_join_lock;
std::mutex BroadCastJoinBuilder::join_lock_mutex;

struct StorageJoinContext
{
    std::string key;
    jobject input;
    DB::Names key_names;
    DB::ASTTableJoin::Kind kind;
    DB::ASTTableJoin::Strictness strictness;
    DB::ColumnsDescription columns;
};

void BroadCastJoinBuilder::buildJoinIfNotExist(
    const std::string & key,
    jobject input,
    const DB::Names & key_names_,
    DB::ASTTableJoin::Kind kind_,
    DB::ASTTableJoin::Strictness strictness_,
    const DB::ColumnsDescription & columns_)
{
    if (!storage_join_map.contains(key))
    {
        std::lock_guard build_lock(join_lock_mutex);
        if (!storage_join_map.contains(key))
        {
            StorageJoinContext context
            {
                key, input, key_names_, kind_, strictness_, columns_
            };
            // use another thread, exclude broadcast memory allocation from current memory tracker
            auto func = [context]() -> void
            {
                // limit memory usage
                if (storage_join_queue.size() > 10)
                {
                    auto tmp = storage_join_queue.front();
                    storage_join_queue.pop();
                    storage_join_map.erase(tmp);
                }
                auto storage_join = std::make_shared<StorageJoinFromReadBuffer>(
                    std::make_unique<ReadBufferFromJavaInputStream>(context.input),
                    StorageID("default", context.key),
                    context.key_names,
                    true,
                    SizeLimits(),
                    context.kind,
                    context.strictness,
                    context.columns,
                    ConstraintsDescription(),
                    context.key,
                    true);
                storage_join_map.emplace(context.key, storage_join);
                storage_join_queue.push(context.key);
            };
            ThreadFromGlobalPool build_thread(std::move(func));
            build_thread.join();
        }
    }
}
std::shared_ptr<StorageJoinFromReadBuffer> BroadCastJoinBuilder::getJoin(const std::string & key)
{
    if (storage_join_map.contains(key))
    {
        return storage_join_map.at(key);
    }
    else
    {
        return std::shared_ptr<StorageJoinFromReadBuffer>();
    }
}
 void BroadCastJoinBuilder::buildJoinIfNotExist(
    const std::string & key,
    jobject input,
    const std::string & join_keys,
    const std::string & join_type,
    const std::string & named_struct)
{
    auto join_key_list = Poco::StringTokenizer(join_keys, ",");
    Names key_names;
    for (const auto & key_name : join_key_list)
    {
        key_names.emplace_back(key_name);
    }
    DB::ASTTableJoin::Kind kind;
    DB::ASTTableJoin::Strictness strictness;
    if (join_type == "Inner")
    {
        kind = DB::ASTTableJoin::Kind::Inner;
        strictness = DB::ASTTableJoin::Strictness::All;
    }
    else if (join_type == "Semi")
    {
        kind = DB::ASTTableJoin::Kind::Left;
        strictness = DB::ASTTableJoin::Strictness::Semi;
    }
    else if (join_type == "Anti")
    {
        kind = DB::ASTTableJoin::Kind::Left;
        strictness = DB::ASTTableJoin::Strictness::Anti;
    }
    else if (join_type == "Left")
    {
        kind = DB::ASTTableJoin::Kind::Left;
        strictness = DB::ASTTableJoin::Strictness::All;
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", join_type);
    }

    auto substrait_struct = std::make_unique<substrait::NamedStruct>();
    substrait_struct->ParseFromString(named_struct);

    Block header = SerializedPlanParser::parseNameStruct(*substrait_struct);
    ColumnsDescription columns_description(header.getNamesAndTypesList());
    buildJoinIfNotExist(key, input, key_names, kind, strictness, columns_description);
}
void BroadCastJoinBuilder::clean()
{
    storage_join_lock.clear();
    storage_join_map.clear();
    while (!storage_join_queue.empty())
    {
        storage_join_queue.pop();
    }
}

}
