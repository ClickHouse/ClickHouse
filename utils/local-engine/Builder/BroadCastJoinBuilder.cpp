#include "BroadCastJoinBuilder.h"
#include <Poco/StringTokenizer.h>
#include <Parser/SerializedPlanParser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int UNKNOWN_FUNCTION;
}
}


using namespace DB;

namespace local_engine
{

std::unordered_map<std::string, std::shared_ptr<StorageJoinFromReadBuffer>> BroadCastJoinBuilder::storage_join_map;
std::unordered_map<std::string, std::shared_ptr<std::mutex>> BroadCastJoinBuilder::storage_join_lock;
std::mutex BroadCastJoinBuilder::join_lock_mutex;

void BroadCastJoinBuilder::buildJoinIfNotExist(
    const std::string & key,
    std::unique_ptr<ReadBufferFromJavaInputStream> read_buffer,
    const DB::Names & key_names_,
    DB::ASTTableJoin::Kind kind_,
    DB::ASTTableJoin::Strictness strictness_,
    const DB::ColumnsDescription & columns_)
{
    {
        std::lock_guard join_lock(join_lock_mutex);
        if (!storage_join_lock.contains(key))
        {
            storage_join_lock.emplace(key, std::make_shared<std::mutex>());
        }
    }

    if (!storage_join_map.contains(key))
    {
        std::lock_guard build_lock(*storage_join_lock.at(key));
        if (!storage_join_map.contains(key))
        {
            storage_join_map.emplace(key, std::make_shared<StorageJoinFromReadBuffer>(std::move(read_buffer),
                                                                                     StorageID("default", key),
                                                                                     key_names_,
                                                                                     true,
                                                                                     SizeLimits(),
                                                                                     kind_,
                                                                                     strictness_,
                                                                                     columns_,
                                                                                     ConstraintsDescription(),
                                                                                     key,
                                                                                     true));
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
    std::unique_ptr<ReadBufferFromJavaInputStream> read_buffer,
    const std::string & join_keys,
    const std::string & join_type,
    const std::string & named_struct)
{
    auto join_key_list = Poco::StringTokenizer(join_keys, ",");
    Names key_names;
    for (const auto& key_name : join_key_list)
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
    buildJoinIfNotExist(key, std::move(read_buffer), key_names, kind, strictness, columns_description);
}

}
