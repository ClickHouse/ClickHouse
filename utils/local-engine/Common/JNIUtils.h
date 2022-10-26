#pragma once
#include <jni.h>

namespace local_engine
{
class JNIUtils
{
public:
    static JavaVM * vm;

    static JNIEnv * getENV(int * attach);

    static void detachCurrentThread();
};
#define GET_JNIENV(env) \
    int attached; \
    JNIEnv * (env) = JNIUtils::getENV(&attached);

#define CLEAN_JNIENV \
    if (attached) [[unlikely]]\
    { \
        JNIUtils::detachCurrentThread(); \
    }
}
