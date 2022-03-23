#include "ShuffleSplitter.h"
#include <filesystem>
#include <fcntl.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/BrotliWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedWriteBuffer.h>

namespace local_engine
{
void ShuffleSplitter::split(Block & block)
{
    computeAndCountPartitionId(block);
    splitBlockByPartition(block);
}
void ShuffleSplitter::stop()
{
    // spill all buffers
    for (size_t i = 0; i < options.partition_nums; i++)
    {
        spillPartition(i);
        partition_outputs[i]->flush();
        partition_write_buffers[i].reset();
    }
    partition_outputs.clear();
    partition_cached_write_buffers.clear();
    partition_write_buffers.clear();
    mergePartitionFiles();
    stopped = true;
}
void ShuffleSplitter::splitBlockByPartition(Block & block)
{
    IColumn::Selector selector;
    buildSelector(block.rows(), selector);
    std::vector<Block> partitions;
    for (size_t i = 0; i < options.partition_nums; ++i)
        partitions.emplace_back(block.cloneEmpty());
    for (size_t col = 0; col < block.columns(); ++col)
    {
        MutableColumns scattered = block.getByPosition(col).column->scatter(options.partition_nums, selector);
        for (size_t i = 0; i < options.partition_nums; ++i)
            partitions[i].getByPosition(col).column = std::move(scattered[i]);
    }

    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        ColumnsBuffer & buffer = partition_buffer[i];
        size_t first_cache_count = std::min(partitions[i].rows(), options.buffer_size - buffer.size());
        if (first_cache_count < partitions[i].rows())
        {
            buffer.add(partitions[i], 0, first_cache_count);
            spillPartition(i);
            buffer.add(partitions[i], first_cache_count, partitions[i].rows());
        }
        else
        {
            buffer.add(partitions[i], 0, first_cache_count);
        }
        if (buffer.size() == options.buffer_size)
        {
            spillPartition(i);
        }
    }
}
void ShuffleSplitter::init()
{
    partition_ids.reserve(options.buffer_size);
    partition_buffer.reserve(options.partition_nums);
    partition_outputs.reserve(options.partition_nums);
    partition_write_buffers.reserve(options.partition_nums);
    partition_cached_write_buffers.reserve(options.partition_nums);
    partition_length.reserve(options.partition_nums);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        partition_buffer.emplace_back(ColumnsBuffer());
        partition_length.emplace_back(0);
        partition_outputs.emplace_back(nullptr);
        partition_write_buffers.emplace_back(nullptr);
        partition_cached_write_buffers.emplace_back(nullptr);
    }
}

void ShuffleSplitter::buildSelector(size_t row_nums, IColumn::Selector & selector)
{
    assert(!partition_ids.empty() && "partition ids is empty");
    selector = IColumn::Selector(row_nums);
    selector.assign(partition_ids.begin(), partition_ids.end());
}

void ShuffleSplitter::spillPartition(size_t partition_id)
{
    if (!partition_outputs[partition_id])
    {
        partition_write_buffers[partition_id]
            = getPartitionWriteBuffer(partition_id);
        partition_outputs[partition_id] = std::make_unique<NativeBlockOutputStream>(
            *partition_write_buffers[partition_id], 0, partition_buffer[partition_id].getHeader());
    }
    Block result = partition_buffer[partition_id].releaseColumns();
    partition_outputs[partition_id]->write(result);
}
void ShuffleSplitter::mergePartitionFiles()
{
    WriteBufferFromFile data_write_buffer = WriteBufferFromFile(options.data_file);
    std::string buffer;
    int buffer_size = 1024 * 1024;
    buffer.reserve(buffer_size);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        auto file = getPartitionTempFile(i);
        ReadBufferFromFile reader = ReadBufferFromFile(file);
        partition_length[i] = 0;
        while (reader.next())
        {
            auto bytes = reader.readBig(buffer.data(), buffer_size);
            data_write_buffer.write(buffer.data(), bytes);
            partition_length[i] += bytes;
        }
        reader.close();
        std::filesystem::remove(file);
    }
    data_write_buffer.close();
}
ShuffleSplitter::ShuffleSplitter(SplitOptions&& options_) : options(options_)
{
    init();
}
ShuffleSplitter::Ptr ShuffleSplitter::create(std::string short_name, SplitOptions options_)
{
    if (short_name == "round-robin")
    {
        return RoundRobinSplitter::create(std::move(options_));
    }
    else
    {
        throw "unsupported splitter " + short_name;
    }
}
std::string ShuffleSplitter::getPartitionTempFile(size_t partition_id)
{
    std::string dir = std::filesystem::path(options.local_tmp_dir)/"_shuffle_data"/std::to_string(options.map_id);
    if (!std::filesystem::exists(dir)) std::filesystem::create_directories(dir);
    return std::filesystem::path(dir)/std::to_string(partition_id);
}
std::unique_ptr<WriteBuffer> ShuffleSplitter::getPartitionWriteBuffer(size_t partition_id)
{
    auto file = getPartitionTempFile(partition_id);
    if (partition_cached_write_buffers[partition_id] == nullptr)
        partition_cached_write_buffers[partition_id] = std::make_unique<WriteBufferFromFile>(file, DBMS_DEFAULT_BUFFER_SIZE, O_CREAT | O_WRONLY | O_APPEND);
    if (!options.compress_method.empty() && std::find(compress_methods.begin(), compress_methods.end(), options.compress_method) != compress_methods.end())
    {
        auto codec = CompressionCodecFactory::instance().get(options.compress_method, {});
        return std::make_unique<CompressedWriteBuffer>(*partition_cached_write_buffers[partition_id], codec);
    }
//    if (options.compress_method == "zstd")
//    {
//        return std::make_unique<ZstdDeflatingWriteBuffer>(std::move(file_write_buffer), options.compress_level);
//    }
//    else if (options.compress_method == "zlib")
//    {
//        return std::make_unique<ZlibDeflatingWriteBuffer>(std::move(file_write_buffer), CompressionMethod::Gzip, options.compress_level);
//    }
//    else if (options.compress_method == "lzma")
//    {
//        return std::make_unique<LZMADeflatingWriteBuffer>(std::move(file_write_buffer), options.compress_level);
//    }
//    else if (options.compress_method == "brotli")
//    {
//        return std::make_unique<BrotliWriteBuffer>(std::move(file_write_buffer), options.compress_level);
//    }
    else
    {
        return std::move(partition_cached_write_buffers[partition_id]);
    }
}

const std::vector<std::string> ShuffleSplitter::compress_methods = {"", "ZSTD", "LZ4"};

void ColumnsBuffer::add(Block & block, int start, int end)
{
    if (header.columns() == 0)
        header = block.cloneEmpty();
    if (accumulated_columns.empty())
    {
        accumulated_columns.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); i++)
        {
            accumulated_columns.emplace_back(block.getColumns()[i]->cloneEmpty());
        }
    }
    assert(!accumulated_columns.empty());
    for (size_t i = 0; i < block.columns(); ++i)
        accumulated_columns[i]->insertRangeFrom(*block.getByPosition(i).column, start, end - start);
}

size_t ColumnsBuffer::size() const
{
    if (accumulated_columns.empty())
        return 0;
    return accumulated_columns.at(0)->size();
}

Block ColumnsBuffer::releaseColumns()
{
    Columns res(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    return header.cloneWithColumns(res);
}
Block ColumnsBuffer::getHeader()
{
    return header;
}
void RoundRobinSplitter::computeAndCountPartitionId(Block & block)
{
    partition_ids.resize(block.rows());
    for (auto & pid : partition_ids)
    {
        pid = pid_selection_;
        pid_selection_ = (pid_selection_ + 1) % options.partition_nums;
    }
}
std::unique_ptr<ShuffleSplitter> RoundRobinSplitter::create(SplitOptions&& options_)
{
    return std::make_unique<RoundRobinSplitter>( std::move(options_));
}
}
