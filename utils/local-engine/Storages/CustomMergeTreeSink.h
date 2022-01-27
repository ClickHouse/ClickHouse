#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include "CustomStorageMergeTree.h"

namespace local_engine
{

class CustomMergeTreeSink : public SinkToStorage
{
public:
    CustomMergeTreeSink(
        CustomStorageMergeTree & storage_,
        const StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
    {
    }

    String getName() const override { return "CustomMergeTreeSink"; }
    void consume(Chunk chunk) override;
private:
    CustomStorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};

}

