#include "SourceFromJavaIter.h"
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/DebugUtils.h>
#include <Common/JNIUtils.h>
#include <Columns/ColumnNullable.h>

namespace local_engine
{
jclass SourceFromJavaIter::serialized_record_batch_iterator_class = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_hasNext = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_next = nullptr;

DB::Chunk SourceFromJavaIter::generate()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    jboolean has_next = env->CallBooleanMethod(java_iter,serialized_record_batch_iterator_hasNext);
    DB::Chunk result;
    if (has_next)
    {
        jbyteArray block = static_cast<jbyteArray>(env->CallObjectMethod(java_iter, serialized_record_batch_iterator_next));
        DB::Block * data = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
        if (data->rows() > 0)
        {
            size_t rows = data->rows();
            result.setColumns(data->mutateColumns(), rows);
            convertNullable(result);
            auto info = std::make_shared<DB::AggregatedChunkInfo>();
            info->is_overflows = data->info.is_overflows;
            info->bucket_num = data->info.bucket_num;
            result.setChunkInfo(info);
        }
    }
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
    return result;
}
SourceFromJavaIter::~SourceFromJavaIter()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    env->DeleteGlobalRef(java_iter);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
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
void SourceFromJavaIter::convertNullable(DB::Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    auto output = this->getOutputs().front().getHeader();
    for (size_t i = 0; i < columns.size(); ++i)
    {
        DB::WhichDataType which(columns.at(i)->getDataType());
        if (output.getByPosition(i).type->isNullable()
            && !which.isNullable()
            && !which.isAggregateFunction())
        {
            columns[i] = DB::makeNullable(columns.at(i));
        }
    }
    chunk.setColumns(columns, rows);
}
}
