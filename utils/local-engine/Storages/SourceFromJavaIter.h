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

    SourceFromJavaIter(DB::Block header, jobject java_iter_, JavaVM * vm_): DB::ISource(header),
        java_iter(java_iter_), vm(vm_)
    {
    }
    String getName() const override { return "SourceFromJavaIter"; }
    ~SourceFromJavaIter();
private:

    DB::Chunk generate() override;

    Int64 byteArrayToLong(JNIEnv* env, jbyteArray arr);

    jobject java_iter;
    JavaVM * vm;
};

}
