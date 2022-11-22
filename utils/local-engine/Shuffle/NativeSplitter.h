#pragma once
#include <jni.h>
#include <memory>
#include <mutex>
#include <stack>
#include <Shuffle/ShuffleSplitter.h>
#include <Common/BlockIterator.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <base/types.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Shuffle/SelectorBuilder.h>

namespace local_engine
{
class NativeSplitter : BlockIterator
{
public:
    struct Options
    {
        size_t buffer_size = 8192;
        size_t partition_nums;
        std::string exprs_buffer;
    };

    struct Holder
    {
        std::unique_ptr<NativeSplitter> splitter = nullptr;
    };

    static jclass iterator_class;
    static jmethodID iterator_has_next;
    static jmethodID iterator_next;
    static std::unique_ptr<NativeSplitter> create(const std::string & short_name, Options options, jobject input);

    NativeSplitter(Options options, jobject input);
    bool hasNext();
    DB::Block * next();
    int32_t nextPartitionId();


    virtual ~NativeSplitter();

protected:
    virtual void computePartitionId(DB::Block &) { }
    Options options;
    std::vector<DB::IColumn::ColumnIndex> partition_ids;


private:
    void split(DB::Block & block);
    int64_t inputNext();
    bool inputHasNext();


    std::vector<std::shared_ptr<ColumnsBuffer>> partition_buffer;
    std::stack<std::pair<int32_t, std::unique_ptr<DB::Block>>> output_buffer;
    int32_t next_partition_id = -1;
    jobject input;
};

class HashNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;

public:
    HashNativeSplitter(NativeSplitter::Options options_, jobject input);

private:
    std::unique_ptr<HashSelectorBuilder> selector_builder;
};

class RoundRobinNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;

public:
    RoundRobinNativeSplitter(NativeSplitter::Options options_, jobject input);

private:
    std::unique_ptr<RoundRobinSelectorBuilder> selector_builder;
};

class RangePartitionNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;
public:
    RangePartitionNativeSplitter(NativeSplitter::Options options_, jobject input);
    ~RangePartitionNativeSplitter() override = default;
private:
    std::unique_ptr<RangeSelectorBuilder> selector_builder;
};

}
