#include "WriteBufferFromJavaOutputStream.h"

namespace local_engine
{
jclass WriteBufferFromJavaOutputStream::output_stream_class = nullptr;
jmethodID WriteBufferFromJavaOutputStream::output_stream_write = nullptr;
jmethodID WriteBufferFromJavaOutputStream::output_stream_flush = nullptr;

void WriteBufferFromJavaOutputStream::nextImpl()
{
    JNIEnv * env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK)
    {
        throw std::runtime_error("get env error");
    }

    Position current_pos = this->working_buffer.begin();
    size_t bytes_write = 0;
    while (offset() - bytes_write > 0)
    {
        size_t copy_num = std::min(offset() - bytes_write, buffer_size);
        env->SetByteArrayRegion(buffer, 0 , copy_num, reinterpret_cast<const jbyte *>(this->working_buffer.begin() + bytes_write));
        env->CallVoidMethod(output_stream, output_stream_write, buffer, 0, copy_num);
        bytes_write += copy_num;
    }
}
WriteBufferFromJavaOutputStream::WriteBufferFromJavaOutputStream(JavaVM * vm_, jobject output_stream_, jbyteArray buffer_)
    : vm(vm_)
{
    JNIEnv * env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK)
    {
        throw std::runtime_error("get env error");
    }
    buffer = static_cast<jbyteArray>(env->NewGlobalRef(buffer_));
    output_stream = env->NewGlobalRef(output_stream_);
    buffer_size = env->GetArrayLength(buffer);
}
void WriteBufferFromJavaOutputStream::finalizeImpl()
{
    next();
    JNIEnv * env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK)
    {
        throw std::runtime_error("get env error");
    }
    env->CallVoidMethod(output_stream, output_stream_flush);
}
WriteBufferFromJavaOutputStream::~WriteBufferFromJavaOutputStream()
{
    JNIEnv * env;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    env->DeleteGlobalRef(output_stream);
    env->DeleteGlobalRef(buffer);
}
}
