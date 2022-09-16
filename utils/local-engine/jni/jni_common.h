#pragma once
#include <jni.h>

namespace local_engine
{
jclass CreateGlobalExceptionClassReference(JNIEnv *env, const char *class_name);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name);

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig);

jmethodID GetStaticMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig);

jstring charTojstring(JNIEnv* env, const char* pat);

}
