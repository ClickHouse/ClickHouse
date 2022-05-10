#pragma once

#include <Processors/ISink.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include "CustomStorageMergeTree.h"

namespace local_engine
{

class CustomMergeTreeSink : public ISink
{
public:
    CustomMergeTreeSink(
        CustomStorageMergeTree & storage_,
        const StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_)
        : ISink(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
    {
    }

    String getName() const override { return "CustomMergeTreeSink"; }
    void consume(Chunk chunk) override;
//    std::list<OutputPort> getOutputs();
private:
    CustomStorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};

}

