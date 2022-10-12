#include "ShuffleSplitter.h"
#include <filesystem>
#include <fcntl.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Functions/FunctionFactory.h>
#include <IO/BrotliWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parser/SerializedPlanParser.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/DebugUtils.h>


namespace local_engine
{
void ShuffleSplitter::split(DB::Block & block)
{
    Stopwatch watch;
    watch.start();
    computeAndCountPartitionId(block);
    splitBlockByPartition(block);
    split_result.total_write_time += watch.elapsedNanoseconds();
}
SplitResult ShuffleSplitter::stop()
{
    // spill all buffers
    Stopwatch watch;
    watch.start();
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
    split_result.total_write_time += watch.elapsedNanoseconds();
    stopped = true;
    return split_result;
}
void ShuffleSplitter::splitBlockByPartition(DB::Block & block)
{
    DB::IColumn::Selector selector;
    buildSelector(block.rows(), selector);
    std::vector<DB::Block> partitions;
    for (size_t i = 0; i < options.partition_nums; ++i)
        partitions.emplace_back(block.cloneEmpty());
    for (size_t col = 0; col < block.columns(); ++col)
    {
        DB::MutableColumns scattered = block.getByPosition(col).column->scatter(options.partition_nums, selector);
        for (size_t i = 0; i < options.partition_nums; ++i)
            partitions[i].getByPosition(col).column = std::move(scattered[i]);
    }

    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        split_result.raw_partition_length[i] += partitions[i].bytes();
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
    split_result.partition_length.reserve(options.partition_nums);
    split_result.raw_partition_length.reserve(options.partition_nums);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        partition_buffer.emplace_back(ColumnsBuffer());
        split_result.partition_length.emplace_back(0);
        split_result.raw_partition_length.emplace_back(0);
        partition_outputs.emplace_back(nullptr);
        partition_write_buffers.emplace_back(nullptr);
        partition_cached_write_buffers.emplace_back(nullptr);
    }
}

void ShuffleSplitter::buildSelector(size_t row_nums, DB::IColumn::Selector & selector)
{
    assert(!partition_ids.empty() && "partition ids is empty");
    selector = DB::IColumn::Selector(row_nums);
    selector.assign(partition_ids.begin(), partition_ids.end());
}

void ShuffleSplitter::spillPartition(size_t partition_id)
{
    Stopwatch watch;
    watch.start();
    if (!partition_outputs[partition_id])
    {
        partition_write_buffers[partition_id] = getPartitionWriteBuffer(partition_id);
        partition_outputs[partition_id]
            = std::make_unique<DB::NativeWriter>(*partition_write_buffers[partition_id], 0, partition_buffer[partition_id].getHeader());
    }
    DB::Block result = partition_buffer[partition_id].releaseColumns();
    if (result.rows() > 0)
    {
        partition_outputs[partition_id]->write(result);
    }
    split_result.total_spill_time += watch.elapsedNanoseconds();
    split_result.total_bytes_spilled += result.bytes();
}

void ShuffleSplitter::mergePartitionFiles()
{
    DB::WriteBufferFromFile data_write_buffer = DB::WriteBufferFromFile(options.data_file);
    std::string buffer;
    int buffer_size = 1024 * 1024;
    buffer.reserve(buffer_size);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        auto file = getPartitionTempFile(i);
        DB::ReadBufferFromFile reader = DB::ReadBufferFromFile(file);
        while (reader.next())
        {
            auto bytes = reader.readBig(buffer.data(), buffer_size);
            data_write_buffer.write(buffer.data(), bytes);
            split_result.partition_length[i] += bytes;
            split_result.total_bytes_written += bytes;
        }
        reader.close();
        std::filesystem::remove(file);
    }
    data_write_buffer.close();
}

ShuffleSplitter::ShuffleSplitter(SplitOptions && options_) : options(options_)
{
    init();
}

ShuffleSplitter::Ptr ShuffleSplitter::create(const std::string & short_name, SplitOptions options_)
{
    if (short_name == "rr")
    {
        return RoundRobinSplitter::create(std::move(options_));
    }
    else if (short_name == "hash")
    {
        return HashSplitter::create(std::move(options_));
    }
    else if (short_name == "single")
    {
        options_.partition_nums = 1;
        return RoundRobinSplitter::create(std::move(options_));
    }
    else
    {
        throw std::runtime_error("unsupported splitter " + short_name);
    }
}

std::string ShuffleSplitter::getPartitionTempFile(size_t partition_id)
{
    std::string dir = std::filesystem::path(options.local_tmp_dir) / "_shuffle_data" / std::to_string(options.map_id);
    if (!std::filesystem::exists(dir))
        std::filesystem::create_directories(dir);
    return std::filesystem::path(dir) / std::to_string(partition_id);
}
std::unique_ptr<DB::WriteBuffer> ShuffleSplitter::getPartitionWriteBuffer(size_t partition_id)
{
    auto file = getPartitionTempFile(partition_id);
    if (partition_cached_write_buffers[partition_id] == nullptr)
        partition_cached_write_buffers[partition_id]
            = std::make_unique<DB::WriteBufferFromFile>(file, DBMS_DEFAULT_BUFFER_SIZE, O_CREAT | O_WRONLY | O_APPEND);
    if (!options.compress_method.empty()
        && std::find(compress_methods.begin(), compress_methods.end(), options.compress_method) != compress_methods.end())
    {
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(options.compress_method), {});
        return std::make_unique<DB::CompressedWriteBuffer>(*partition_cached_write_buffers[partition_id], codec);
    }
    else
    {
        return std::move(partition_cached_write_buffers[partition_id]);
    }
}

const std::vector<std::string> ShuffleSplitter::compress_methods = {"", "ZSTD", "LZ4"};

void ShuffleSplitter::writeIndexFile()
{
    auto index_file = options.data_file + ".index";
    auto writer = std::make_unique<DB::WriteBufferFromFile>(index_file, DBMS_DEFAULT_BUFFER_SIZE, O_CREAT | O_WRONLY | O_TRUNC);
    for (auto len : split_result.partition_length)
    {
        DB::writeIntText(len, *writer);
        DB::writeChar('\n', *writer);
    }
}

void ColumnsBuffer::add(DB::Block & block, int start, int end)
{
    if (header.columns() == 0)
        header = block.cloneEmpty();
    if (accumulated_columns.empty())
    {
        accumulated_columns.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto column = block.getColumns()[i]->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
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

DB::Block ColumnsBuffer::releaseColumns()
{
    DB::Columns res(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    if (res.empty())
    {
        return header.cloneEmpty();
    }
    else
    {
        return header.cloneWithColumns(res);
    }
}

DB::Block ColumnsBuffer::getHeader()
{
    return header;
}
ColumnsBuffer::ColumnsBuffer(size_t prefer_buffer_size_) : prefer_buffer_size(prefer_buffer_size_)
{
}

void RoundRobinSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    watch.start();
    partition_ids.resize(block.rows());
    for (auto & pid : partition_ids)
    {
        pid = pid_selection;
        pid_selection = (pid_selection + 1) % options.partition_nums;
    }
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}

std::unique_ptr<ShuffleSplitter> RoundRobinSplitter::create(SplitOptions && options_)
{
    return std::make_unique<RoundRobinSplitter>(std::move(options_));
}

std::unique_ptr<ShuffleSplitter> HashSplitter::create(SplitOptions && options_)
{
    return std::make_unique<HashSplitter>(std::move(options_));
}

void HashSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    watch.start();
    ColumnsWithTypeAndName args;
    for (auto &name : options.exprs)
    {
        args.emplace_back(block.getByName(name));
    }
    if (!hash_function)
    {
        auto & factory = DB::FunctionFactory::instance();
        auto function = factory.get("murmurHash3_32", local_engine::SerializedPlanParser::global_context);

        hash_function = function->build(args);
    }
    auto result_type = hash_function->getResultType();
    auto hash_column = hash_function->execute(args, result_type, block.rows(), false);
    partition_ids.clear();
    for (size_t i = 0; i < block.rows(); i++)
    {
        partition_ids.emplace_back(static_cast<UInt64>(hash_column->get64(i)  % options.partition_nums));
    }
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}

}
