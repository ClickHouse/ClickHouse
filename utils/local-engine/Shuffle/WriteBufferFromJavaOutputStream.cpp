#include "WriteBufferFromJavaOutputStream.h"
#include <Common/JNIUtils.h>

namespace local_engine
{
jclass WriteBufferFromJavaOutputStream::output_stream_class = nullptr;
jmethodID WriteBufferFromJavaOutputStream::output_stream_write = nullptr;
jmethodID WriteBufferFromJavaOutputStream::output_stream_flush = nullptr;

void WriteBufferFromJavaOutputStream::nextImpl()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);

    size_t bytes_write = 0;
    while (offset() - bytes_write > 0)
    {
        size_t copy_num = std::min(offset() - bytes_write, buffer_size);
        env->SetByteArrayRegion(buffer, 0 , copy_num, reinterpret_cast<const jbyte *>(this->working_buffer.begin() + bytes_write));
        env->CallVoidMethod(output_stream, output_stream_write, buffer, 0, copy_num);
        bytes_write += copy_num;
    }
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}
WriteBufferFromJavaOutputStream::WriteBufferFromJavaOutputStream(jobject output_stream_, jbyteArray buffer_)
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    buffer = static_cast<jbyteArray>(env->NewWeakGlobalRef(buffer_));
    output_stream = env->NewWeakGlobalRef(output_stream_);
    buffer_size = env->GetArrayLength(buffer);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}
void WriteBufferFromJavaOutputStream::finalizeImpl()
{
    next();
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    env->CallVoidMethod(output_stream, output_stream_flush);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}
WriteBufferFromJavaOutputStream::~WriteBufferFromJavaOutputStream()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    env->DeleteWeakGlobalRef(output_stream);
    env->DeleteWeakGlobalRef(buffer);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}
}
