#pragma once
#include <Storages/CustomStorageMergeTree.h>

namespace local_engine
{
using CustomStorageMergeTreePtr = std::shared_ptr<CustomStorageMergeTree>;
using StorageInMemoryMetadataPtr = std::shared_ptr<DB::StorageInMemoryMetadata>;

class StorageMergeTreeFactory
{
public:
    static StorageMergeTreeFactory & instance();
    static CustomStorageMergeTreePtr getStorage(StorageID id, ColumnsDescription columns, std::function<CustomStorageMergeTreePtr()> creator);
    static StorageInMemoryMetadataPtr getMetadata(StorageID id, std::function<StorageInMemoryMetadataPtr()> creator);

private:
    static std::unordered_map<std::string , CustomStorageMergeTreePtr> storage_map;
    static std::unordered_map<std::string , std::set<std::string>> storage_columns_map;
    static std::mutex storage_map_mutex;

    static std::unordered_map<std::string , StorageInMemoryMetadataPtr> metadata_map;
    static std::mutex metadata_map_mutex;
};
}
