#include "NativeSplitter.h"
#include <functional>
#include <memory>
#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/Exception.h>
#include <boost/asio/detail/eventfd_select_interrupter.hpp>
#include <jni/jni_common.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>
#include <Core/Block.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Poco/StringTokenizer.h>
#include <string>

namespace local_engine
{
jclass NativeSplitter::iterator_class = nullptr;
jmethodID NativeSplitter::iterator_has_next = nullptr;
jmethodID NativeSplitter::iterator_next = nullptr;

void NativeSplitter::split(DB::Block & block)
{
    computePartitionId(block);
    DB::IColumn::Selector selector;
    selector = DB::IColumn::Selector(block.rows());
    selector.assign(partition_ids.begin(), partition_ids.end());
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
        auto buffer = partition_buffer[i];
        size_t first_cache_count = std::min(partitions[i].rows(), options.buffer_size - buffer->size());
        if (first_cache_count < partitions[i].rows())
        {
            buffer->add(partitions[i], 0, first_cache_count);
            output_buffer.emplace(std::pair(i, std::make_unique<Block>(buffer->releaseColumns())));
            buffer->add(partitions[i], first_cache_count, partitions[i].rows());
        }
        else
        {
            buffer->add(partitions[i], 0, first_cache_count);
        }
        if (buffer->size() >= options.buffer_size)
        {
            output_buffer.emplace(std::pair(i, std::make_unique<Block>(buffer->releaseColumns())));
        }
    }
}

NativeSplitter::NativeSplitter(Options options_, jobject input_) : options(options_)
{
    GET_JNIENV(env)
    input = env->NewGlobalRef(input_);
    partition_ids.reserve(options.buffer_size);
    partition_buffer.reserve(options.partition_nums);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        partition_buffer.emplace_back(std::make_shared<ColumnsBuffer>());
    }
    CLEAN_JNIENV
}

NativeSplitter::~NativeSplitter()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(input);
    CLEAN_JNIENV
}

bool NativeSplitter::hasNext()
{
    while (output_buffer.empty())
    {
        if (inputHasNext())
        {
            split(*reinterpret_cast<Block *>(inputNext()));
        }
        else
        {
            for (size_t i = 0; i < options.partition_nums; ++i)
            {
                auto buffer = partition_buffer.at(i);
                if (buffer->size() > 0)
                {
                    output_buffer.emplace(std::pair(i, new Block(buffer->releaseColumns())));
                }
            }
            break;
        }
    }
    if (!output_buffer.empty())
    {
        next_partition_id = output_buffer.top().first;
        setCurrentBlock(*output_buffer.top().second);
        produce();
    }
    return !output_buffer.empty();
}

DB::Block * NativeSplitter::next()
{
    if (!output_buffer.empty())
    {
        output_buffer.pop();
    }
    consume();
    return &currentBlock();
}

int32_t NativeSplitter::nextPartitionId()
{
    return next_partition_id;
}

bool NativeSplitter::inputHasNext()
{
    GET_JNIENV(env)
    bool next = safeCallBooleanMethod(env, input, iterator_has_next);
    CLEAN_JNIENV
    return next;
}

int64_t NativeSplitter::inputNext()
{
    GET_JNIENV(env)
    int64_t result = safeCallLongMethod(env, input, iterator_next);
    CLEAN_JNIENV
    return result;
}
std::unique_ptr<NativeSplitter> NativeSplitter::create(const std::string & short_name, Options options_, jobject input)
{
    if (short_name == "rr")
    {
        return std::make_unique<RoundRobinNativeSplitter>(options_, input);
    }
    else if (short_name == "hash")
    {
        return std::make_unique<HashNativeSplitter>(options_, input);
    }
    else if (short_name == "single")
    {
        options_.partition_nums = 1;
        return std::make_unique<RoundRobinNativeSplitter>(options_, input);
    }
    else if (short_name == "range")
    {
        return std::make_unique<RangePartitionNativeSplitter>(options_, input);
    }
    else
    {
        throw std::runtime_error("unsupported splitter " + short_name);
    }
}

HashNativeSplitter::HashNativeSplitter(NativeSplitter::Options options_, jobject input)
    : NativeSplitter(options_, input)
{
    Poco::StringTokenizer exprs_list(options_.exprs_buffer, ",");
    std::vector<std::string> hash_fields;
    hash_fields.insert(hash_fields.end(), exprs_list.begin(), exprs_list.end());

    selector_builder = std::make_unique<HashSelectorBuilder>(options.partition_nums, hash_fields, "murmurHash3_32");
}

void HashNativeSplitter::computePartitionId(Block & block)
{
    partition_ids = selector_builder->build(block);
}

RoundRobinNativeSplitter::RoundRobinNativeSplitter(NativeSplitter::Options options_, jobject input) : NativeSplitter(options_, input)
{
    selector_builder = std::make_unique<RoundRobinSelectorBuilder>(options_.partition_nums);
}

void RoundRobinNativeSplitter::computePartitionId(Block & block)
{
    partition_ids = selector_builder->build(block);
}

RangePartitionNativeSplitter::RangePartitionNativeSplitter(NativeSplitter::Options options_, jobject input)
    : NativeSplitter(options_, input)
{
    selector_builder = std::make_unique<RangeSelectorBuilder>(options_.exprs_buffer);
}

void RangePartitionNativeSplitter::computePartitionId(DB::Block & block)
{
    partition_ids = selector_builder->build(block);
}

}
