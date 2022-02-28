#pragma once
#include "CustomStorageMergeTree.h"

namespace local_engine
{
using CustomStorageMergeTreePtr = std::shared_ptr<CustomStorageMergeTree>;

class StorageMergeTreeFactory
{
public:
    static StorageMergeTreeFactory & instance();
    static CustomStorageMergeTreePtr getStorage(StorageID id, std::function<CustomStorageMergeTreePtr()> creator);

private:
    static std::unordered_map<std::string , CustomStorageMergeTreePtr> storage_map;
    static std::mutex storage_map_mutex;
};
}



