#include "ReservationListenerWrapper.h"
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

namespace local_engine
{
jclass ReservationListenerWrapper::reservation_listener_class = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_reserve = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_reserve_or_throw = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_unreserve = nullptr;

ReservationListenerWrapper::ReservationListenerWrapper(jobject listener_) : listener(listener_)
{
}

ReservationListenerWrapper::~ReservationListenerWrapper()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(listener);
    CLEAN_JNIENV
}

void ReservationListenerWrapper::reserve(int64_t size)
{
    GET_JNIENV(env)
    safeCallVoidMethod(env, listener, reservation_listener_reserve, size);
    CLEAN_JNIENV
}

void ReservationListenerWrapper::reserveOrThrow(int64_t size)
{
    GET_JNIENV(env)
    safeCallVoidMethod(env, listener, reservation_listener_reserve_or_throw, size);
    CLEAN_JNIENV

}

void ReservationListenerWrapper::free(int64_t size)
{
    GET_JNIENV(env)
    safeCallVoidMethod(env, listener, reservation_listener_unreserve, size);
    CLEAN_JNIENV
}
}
