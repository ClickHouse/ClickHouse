#include "StorageMergeTreeFactory.h"

namespace local_engine
{

StorageMergeTreeFactory & StorageMergeTreeFactory::instance()
{
    static StorageMergeTreeFactory ret;
    return ret;
}

CustomStorageMergeTreePtr
StorageMergeTreeFactory::getStorage(StorageID id, ColumnsDescription columns, std::function<CustomStorageMergeTreePtr()> creator)
{
    auto table_name = id.database_name + "." + id.table_name;
    if (!storage_map.contains(table_name))
    {
        std::lock_guard lock(storage_map_mutex);
        if (storage_map.contains(table_name))
        {
            std::set<std::string> existed_columns = storage_columns_map.at(table_name);
            for (const auto & column : columns)
            {
                if (!existed_columns.contains(column.name))
                {
                    storage_map.erase(table_name);
                    storage_columns_map.erase(table_name);
                }
            }
        }
        if (!storage_map.contains(table_name))
        {
            storage_map.emplace(table_name, creator());
            storage_columns_map.emplace(table_name, std::set<std::string>());
            for (const auto & column : storage_map.at(table_name)->getInMemoryMetadataPtr()->columns)
            {
                storage_columns_map.at(table_name).emplace(column.name);
            }
        }
    }
    return storage_map.at(table_name);
}

StorageInMemoryMetadataPtr StorageMergeTreeFactory::getMetadata(StorageID id, std::function<StorageInMemoryMetadataPtr()> creator)
{
    auto table_name = id.database_name + "." + id.table_name;

    if (!metadata_map.contains(table_name))
    {
        std::lock_guard lock(metadata_map_mutex);
        if (!metadata_map.contains(table_name))
        {
            metadata_map.emplace(table_name, creator());
        }
    }
    return metadata_map.at(table_name);
}


std::unordered_map<std::string, CustomStorageMergeTreePtr> StorageMergeTreeFactory::storage_map;
std::unordered_map<std::string, std::set<std::string>> StorageMergeTreeFactory::storage_columns_map;
std::mutex StorageMergeTreeFactory::storage_map_mutex;

std::unordered_map<std::string, StorageInMemoryMetadataPtr> StorageMergeTreeFactory::metadata_map;
std::mutex StorageMergeTreeFactory::metadata_map_mutex;

}
