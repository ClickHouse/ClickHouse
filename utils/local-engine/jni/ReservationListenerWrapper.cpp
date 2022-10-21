#include "ReservationListenerWrapper.h"
#include <Common/JNIUtils.h>
#include <jni/jni_common.h>

namespace local_engine
{
jclass ReservationListenerWrapper::reservation_listener_class= nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_reserve = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_unreserve = nullptr;

ReservationListenerWrapper::ReservationListenerWrapper(jobject listener_) : listener(listener_)
{
}

ReservationListenerWrapper::~ReservationListenerWrapper()
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    env->DeleteGlobalRef(listener);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}

void ReservationListenerWrapper::reserve(int64_t size)
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    safeCallVoidMethod(env, listener, reservation_listener_reserve, size);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}

void ReservationListenerWrapper::free(int64_t size)
{
    int attached;
    JNIEnv * env = JNIUtils::getENV(&attached);
    safeCallVoidMethod(env, listener, reservation_listener_unreserve, size);
    if (attached)
    {
        JNIUtils::detachCurrentThread();
    }
}
}

