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

struct SplitOptions
{
    size_t buffer_size = DEFAULT_BLOCK_SIZE;
    std::string data_file;
    std::string local_tmp_dir;
    int map_id;
    size_t partition_nums;
    std::string compress_method = "zstd";
    int compress_level;
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
    static const std::vector<std::string> compress_methods;
    using Ptr = std::unique_ptr<ShuffleSplitter>;
    static Ptr create(std::string short_name, SplitOptions options_);
    explicit ShuffleSplitter(SplitOptions && options);
    virtual ~ShuffleSplitter()
    {
        if (!stopped) stop();
    }
    void split(Block & block);
    virtual void computeAndCountPartitionId(Block & block) {}
    std::vector<int64_t> getPartitionLength() {
        return partition_length;
    }
    void stop();

private:
    void init();
    void splitBlockByPartition(Block & block);
    void buildSelector(size_t row_nums, IColumn::Selector & selector);
    void spillPartition(size_t partition_id);
    std::string getPartitionTempFile(size_t partition_id);
    void mergePartitionFiles();
    std::unique_ptr<WriteBuffer> getPartitionWriteBuffer(size_t partition_id);

protected:
    bool stopped = false;
    std::vector<IColumn::ColumnIndex> partition_ids;
    std::vector<ColumnsBuffer> partition_buffer;
    std::vector<std::unique_ptr<NativeBlockOutputStream>> partition_outputs;
    std::vector<std::unique_ptr<WriteBuffer>> partition_write_buffers;
    std::vector<std::unique_ptr<WriteBuffer>> partition_cached_write_buffers;
    SplitOptions options;
    std::vector<int64_t> partition_length;
};

class RoundRobinSplitter : public ShuffleSplitter {
public:
    static std::unique_ptr<ShuffleSplitter> create(SplitOptions && options);

    RoundRobinSplitter(
                       SplitOptions options_)
        : ShuffleSplitter(std::move(options_)) {}

    ~RoundRobinSplitter() override = default;
    void computeAndCountPartitionId(Block & block) override;

private:
    int32_t pid_selection_ = 0;
};
}
