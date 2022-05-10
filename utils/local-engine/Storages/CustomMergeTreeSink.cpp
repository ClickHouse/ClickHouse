#include "CustomMergeTreeSink.h"

void local_engine::CustomMergeTreeSink::consume(Chunk chunk)
{
    auto block = metadata_snapshot->getSampleBlock().cloneWithColumns(chunk.detachColumns());
    DB::BlockWithPartition block_with_partition(Block(block), DB::Row{});
    auto part = storage.writer.writeTempPart(block_with_partition, metadata_snapshot, context);
    storage.renameTempPartAndAdd(part.part, &storage.increment, nullptr, nullptr);
}
//std::list<OutputPort> local_engine::CustomMergeTreeSink::getOutputs()
//{
//    return {};
//}
