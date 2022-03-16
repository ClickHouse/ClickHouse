#pragma once
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Common/PODArray_fwd.h>
#include <Common/PODArray.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/WriteBufferFromFile.h>



using namespace DB;

namespace local_engine
{

struct SplitterOptions
{
    size_t buffer_size = 1024 * 8;
    std::string data_file;
    std::string local_tmp_dir;
    size_t partition_nums;

};

class ColumnsBuffer
{
public:
    void add(Block & columns, int start, int end);
    size_t size() const;
    Block releaseColumns();
    Block getHeader();

private:
    MutableColumns accumulated_columns;
    Block header;
};


class ShuffleSplitter
{
public:
    using Ptr = std::unique_ptr<ShuffleSplitter>;
    Ptr create(SplitterOptions options);
    void split(Block & block);
    virtual void computeAndCountPartitionId(Block & block);
    void stop();

private:
    void init();
    void splitBlockByPartition(Block & block);
    void buildSelector(size_t row_nums, IColumn::Selector & selector);
    void spillPartition(size_t partition_id);
    std::string getPartitionTempFile(size_t partition_id);
    void mergePartitionFiles();

private:
    bool stopped = false;
    std::vector<IColumn::ColumnIndex> partition_id_;
    std::vector<int32_t> partition_id_cnt_;
    std::vector<ColumnsBuffer> partition_buffer;
    std::vector<std::unique_ptr<NativeBlockOutputStream>> partition_outputs;
    std::vector<std::unique_ptr<WriteBufferFromFile>> partition_write_buffers;
    SplitterOptions options;
    std::vector<long> partition_length;
};

}
