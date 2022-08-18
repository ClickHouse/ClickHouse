#include "NativeSplitter.h"
#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/JNIUtils.h>


using namespace DB;
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
            output_buffer.emplace(std::pair(i, new Block(buffer->releaseColumns())));
            buffer->add(partitions[i], first_cache_count, partitions[i].rows());
        }
        else
        {
            buffer->add(partitions[i], 0, first_cache_count);
        }
        if (buffer->size() >= options.buffer_size)
        {
            output_buffer.emplace(std::pair(i, new Block(buffer->releaseColumns())));
        }
    }
}

NativeSplitter::NativeSplitter(Options options_, jobject input_) : options(options_)
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    input = env->NewGlobalRef(input_);
    partition_ids.reserve(options.buffer_size);
    partition_buffer.reserve(options.partition_nums);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        partition_buffer.emplace_back(std::make_shared<ColumnsBuffer>());
    }
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}
NativeSplitter::~NativeSplitter()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    env->DeleteGlobalRef(input);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
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
        next_block = output_buffer.top().second;
    }
    return !output_buffer.empty();
}
DB::Block * NativeSplitter::next()
{
    if (!output_buffer.empty()) {
        output_buffer.pop();
    }
    return next_block;
}
int32_t NativeSplitter::nextPartitionId()
{
    return next_partition_id;
}

bool NativeSplitter::inputHasNext()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    bool next = env->CallBooleanMethod(input, iterator_has_next);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
    return next;
}

int64_t NativeSplitter::inputNext()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    int64_t result = env->CallLongMethod(input, iterator_next);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
    return result;
}
std::unique_ptr<NativeSplitter> NativeSplitter::create(std::string short_name, Options options_, jobject input)
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
    else
    {
        throw std::runtime_error("unsupported splitter " + short_name);
    }
}

void HashNativeSplitter::computePartitionId(Block & block)
{
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
        partition_ids.emplace_back(static_cast<UInt64>(hash_column->get64(i) % options.partition_nums));
    }
}
void RoundRobinNativeSplitter::computePartitionId(Block & block)
{
    partition_ids.resize(block.rows());
    for (auto & pid : partition_ids)
    {
        pid = pid_selection;
        pid_selection = (pid_selection + 1) % options.partition_nums;
    }
}
}
