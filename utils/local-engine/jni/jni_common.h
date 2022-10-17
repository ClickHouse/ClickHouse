#pragma once
#include <exception>
#include <stdexcept>
#include <jni.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
jclass CreateGlobalExceptionClassReference(JNIEnv *env, const char *class_name);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name);

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig);

jmethodID GetStaticMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig);

jstring charTojstring(JNIEnv* env, const char* pat);

#define LOCAL_ENGINE_JNI_JMETHOD_START
#define LOCAL_ENGINE_JNI_JMETHOD_END(env) \
    if ((env)->ExceptionCheck())\
    {\
        (env)->ExceptionDescribe();\
        (env)->ExceptionClear();\
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Call java method failed");\
    }

template <typename ... Args>
jobject safeCallObjectMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallObjectMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

template <typename ... Args>
jboolean safeCallBooleanMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallBooleanMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename ... Args>
jlong safeCallLongMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallLongMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename ... Args>
jint safeCallIntMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallIntMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename ... Args>
void safeCallVoidMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    env->CallVoidMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
}
}
