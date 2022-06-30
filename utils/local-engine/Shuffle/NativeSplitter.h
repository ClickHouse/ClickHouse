#pragma once
#include <jni.h>
#include <stack>
#include <Shuffle/ShuffleSplitter.h>

namespace local_engine
{
class NativeSplitter
{
public:
    struct Options
    {
        size_t buffer_size = 8192;
        size_t partition_nums;
        std::vector<std::string> exprs;
    };

    struct Holder
    {
        std::unique_ptr<NativeSplitter> splitter = nullptr;
    };

    static jclass iterator_class;
    static jmethodID iterator_has_next;
    static jmethodID iterator_next;
    static std::unique_ptr<NativeSplitter> create(std::string short_name, Options options, jobject input, JavaVM * vm);

    NativeSplitter(Options options, jobject input, JavaVM * vm);
    bool hasNext();
    DB::Block * next();
    int32_t nextPartitionId();


    virtual ~NativeSplitter();

protected:
    virtual void computePartitionId(DB::Block & block) { }
    Options options;
    std::vector<DB::IColumn::ColumnIndex> partition_ids;


private:
    void split(DB::Block & block);
    int64_t inputNext();
    bool inputHasNext();


    std::vector<std::shared_ptr<ColumnsBuffer>> partition_buffer;
    std::stack<std::pair<int32_t, DB::Block *>> output_buffer;
    int32_t next_partition_id = -1;
    DB::Block * next_block = nullptr;
    jobject input;
    JavaVM * vm;
};

class HashNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;

public:
    HashNativeSplitter(NativeSplitter::Options options_, jobject input, JavaVM * vm) : NativeSplitter(options_, input, vm) { }

private:
    DB::FunctionBasePtr hash_function;
};

class RoundRobinNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;

public:
    RoundRobinNativeSplitter(NativeSplitter::Options options_, jobject input, JavaVM * vm) : NativeSplitter(options_, input, vm) { }

private:
    int32_t pid_selection = 0;
};

}
