#include "JNIUtils.h"

namespace local_engine
{
JavaVM * JNIUtils::vm = nullptr;

JNIEnv * JNIUtils::getENV(int * attach)
{
    if (vm == nullptr)
        return nullptr;

    *attach = 0;
    JNIEnv * jni_env = nullptr;

    int status = vm->GetEnv(reinterpret_cast<void **>(&jni_env), JNI_VERSION_1_8);

    if (status == JNI_EDETACHED || jni_env == nullptr)
    {
        status = vm->AttachCurrentThread(reinterpret_cast<void **>(&jni_env), nullptr);
        if (status < 0)
        {
            jni_env = nullptr;
        }
        else
        {
            *attach = 1;
        }
    }
    return jni_env;
}
void JNIUtils::detachCurrentThread()
{
    vm->DetachCurrentThread();
}
}
