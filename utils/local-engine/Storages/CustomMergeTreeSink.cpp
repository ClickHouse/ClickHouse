#include "CustomMergeTreeSink.h"

void local_engine::CustomMergeTreeSink::consume(Chunk chunk)
{
    auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
    DB::BlockWithPartition block_with_partition(Block(block), DB::Row{});
    MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(block_with_partition, metadata_snapshot, context);
    storage.renameTempPartAndAdd(part, &storage.increment, nullptr, nullptr);
}
