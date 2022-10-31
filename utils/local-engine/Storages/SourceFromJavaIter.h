#pragma once
#include <Processors/ISource.h>
#include <jni.h>

namespace local_engine
{
class SourceFromJavaIter : public DB::ISource
{
public:
    static jclass serialized_record_batch_iterator_class;
    static jmethodID serialized_record_batch_iterator_hasNext;
    static jmethodID serialized_record_batch_iterator_next;

    static Int64 byteArrayToLong(JNIEnv * env, jbyteArray arr);

    SourceFromJavaIter(DB::Block header, jobject java_iter_);
    ~SourceFromJavaIter() override;

    String getName() const override { return "SourceFromJavaIter"; }

private:
    DB::Chunk generate() override;
    void convertNullable(DB::Chunk & chunk);

    jobject java_iter;
    DB::Block original_header;
};

}
