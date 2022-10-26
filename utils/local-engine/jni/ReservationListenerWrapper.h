#pragma once
#include <stdint.h>
#include <jni.h>
#include <memory>

namespace local_engine
{
class ReservationListenerWrapper
{
public:
    static jclass reservation_listener_class;
    static jmethodID reservation_listener_reserve;
    static jmethodID reservation_listener_reserve_or_throw;
    static jmethodID reservation_listener_unreserve;

    explicit ReservationListenerWrapper(jobject listener);
    ~ReservationListenerWrapper();
    void reserve(int64_t size);
    void reserveOrThrow(int64_t size);
    void free(int64_t size);

private:
    jobject listener;
};
using ReservationListenerWrapperPtr = std::shared_ptr<ReservationListenerWrapper>;
}
