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

    static Int64 byteArrayToLong(JNIEnv* env, jbyteArray arr);

    static JNIEnv * getENV(JavaVM * vm, int *attach) {
        if (vm == nullptr) return nullptr;

        *attach = 0;
        JNIEnv *jni_env = nullptr;

        int status = vm->GetEnv(reinterpret_cast<void **>(&jni_env), JNI_VERSION_1_8);

        if (status == JNI_EDETACHED || jni_env == nullptr) {
            status = vm->AttachCurrentThread(reinterpret_cast<void **>(&jni_env), nullptr);
            if (status < 0) {
                jni_env = nullptr;
            } else {
                *attach = 1;
            }
        }
        return jni_env;
    }

    SourceFromJavaIter(DB::Block header, jobject java_iter_, JavaVM * vm_): DB::ISource(header),
        java_iter(java_iter_), vm(vm_)
    {
    }
    String getName() const override { return "SourceFromJavaIter"; }
    ~SourceFromJavaIter();
private:

    DB::Chunk generate() override;


    jobject java_iter;
    JavaVM * vm;
};

}
