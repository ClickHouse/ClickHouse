#include "ShuffleSplitter.h"
#include <fcntl.h>
#include <IO/ReadBufferFromFile.h>
#include <filesystem>



namespace local_engine
{
void ShuffleSplitter::split(Block & block)
{
}
void ShuffleSplitter::stop()
{
    // spill all buffers
    for (size_t i = 0; i < options.partition_nums; i++)
    {
        spillPartition(i);
        partition_outputs[i]->flush();
        partition_write_buffers[i]->close();
    }
    partition_outputs.clear();
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
}

void ShuffleSplitter::buildSelector(size_t row_nums, IColumn::Selector & selector)
{
    assert(!partition_id_.empty() && "partition ids is empty");
    selector = IColumn::Selector(row_nums);
    selector.assign(partition_id_.begin(), partition_id_.end());
}

void ShuffleSplitter::spillPartition(size_t partition_id)
{
    if (!partition_outputs[partition_id])
    {
        auto file = getPartitionTempFile(partition_id);
        partition_write_buffers[partition_id] = std::make_unique<WriteBufferFromFile>(file, DBMS_DEFAULT_BUFFER_SIZE, O_CREAT | O_WRONLY | O_APPEND);
        partition_outputs[partition_id] = std::make_unique<NativeBlockOutputStream>(*partition_write_buffers[partition_id],
                                                                      0,
                                                                      partition_buffer[partition_id].getHeader());
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
        while(reader.next())
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


void ColumnsBuffer::add(Block & block, int start, int end)
{
    if (header.columns() == 0) header = block.cloneEmpty();
    if (accumulated_columns.empty())
    {
        for (size_t i=0; i < block.columns(); i++)
        {
            accumulated_columns[i] = block.getColumns()[i]->cloneEmpty();
        }
    }
    assert(!accumulated_columns.empty());
    for (size_t i = 0; i < block.columns(); ++i)
        accumulated_columns[i]->insertRangeFrom(*block.getByPosition(i).column, start, end);
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
}
