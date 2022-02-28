#include "StorageMergeTreeFactory.h"

local_engine::StorageMergeTreeFactory & local_engine::StorageMergeTreeFactory::instance()
{
    static local_engine::StorageMergeTreeFactory ret;
    return ret;
}
local_engine::CustomStorageMergeTreePtr
local_engine::StorageMergeTreeFactory::getStorage(StorageID id, std::function<CustomStorageMergeTreePtr()> creator)
{
    auto table_name = id.database_name + "." + id.table_name;
    std::lock_guard lock(storage_map_mutex);
    if (!storage_map.contains(table_name))
    {
        storage_map.emplace(table_name, creator());
    }
    return storage_map.at(table_name);
}

std::unordered_map<std::string , local_engine::CustomStorageMergeTreePtr> local_engine::StorageMergeTreeFactory::storage_map;
std::mutex local_engine::StorageMergeTreeFactory::storage_map_mutex;
