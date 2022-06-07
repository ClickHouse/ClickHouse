#include "SourceFromJavaIter.h"
#include<Processors/Transforms/AggregatingTransform.h>
#include<Common/DebugUtils.h>

namespace local_engine
{
jclass SourceFromJavaIter::serialized_record_batch_iterator_class = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_hasNext = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_next = nullptr;

DB::Chunk SourceFromJavaIter::generate()
{
    int attach;
    JNIEnv * env = getENV(vm, &attach);
    jboolean has_next = env->CallBooleanMethod(java_iter,serialized_record_batch_iterator_hasNext);
    if (has_next)
    {
        jbyteArray block = static_cast<jbyteArray>(env->CallObjectMethod(java_iter, serialized_record_batch_iterator_next));
        DB::Block * data = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
        size_t rows = data->rows();
        auto chunk = DB::Chunk(data->mutateColumns(), rows);
        auto info = std::make_shared<DB::AggregatedChunkInfo>();
        info->is_overflows = data->info.is_overflows;
        info->bucket_num = data->info.bucket_num;
        chunk.setChunkInfo(info);
        return chunk;
    }
    else
    {
        return {};
    }
}
SourceFromJavaIter::~SourceFromJavaIter()
{
    int attach;
    JNIEnv * env = getENV(vm, &attach);
    env->DeleteGlobalRef(java_iter);
}
Int64 SourceFromJavaIter::byteArrayToLong(JNIEnv* env, jbyteArray arr)
{
    jsize len = env->GetArrayLength(arr);
    assert(len == sizeof(Int64));
    char * c_arr = new char[len];
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(c_arr));
    std::reverse(c_arr, c_arr+8);
    Int64 result = reinterpret_cast<Int64 *>(c_arr)[0];
    delete[] c_arr;
    return result;
}
}
