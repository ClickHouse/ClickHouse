#pragma once
#include <jni.h>

namespace local_engine
{
class JNIUtils
{
public:
    static JavaVM * vm;

    static JNIEnv * getENV(int *attach);

    static void detachCurrentThread();
};
}


