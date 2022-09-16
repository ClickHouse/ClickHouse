#pragma once

#include <boost/core/noncopyable.hpp>
#include <mutex>
#include <jni.h>
#include <exception>
#include <Common/Exception.h>
#include <jni/jni_common.h>
#include <boost/stacktrace.hpp>
#include <sstream>
namespace local_engine
{
class JniErrorsGlobalState : boost::noncopyable
{
protected:
    JniErrorsGlobalState() = default;
public:
    ~JniErrorsGlobalState() = default;

    static JniErrorsGlobalState & instance();
    void initialize(JNIEnv * env_);
    void destroy(JNIEnv * env);

    inline jclass getIOExceptionClass() { return io_exception_class; }
    inline jclass getRuntimeExceptionClass() { return runtime_exception_class; }
    inline jclass getUnsupportedOperationExceptionClass() { return unsupportedoperation_exception_class; }
    inline jclass getIllegalAccessExceptionClass() { return illegal_access_exception_class; }
    inline jclass getIllegalArgumentExceptionClass() { return illegal_argument_exception_class; }

    void throwException(JNIEnv * env, const DB::Exception & e);
    void throwException(JNIEnv * env, const std::exception & e);
    static void throwException(JNIEnv * env, jclass exception_class, const std::string & message, const std::string & stack_trace = "");
    void throwRuntimeException(JNIEnv * env, const std::string & message, const std::string & stack_trace = "");


private:
    jclass io_exception_class = nullptr;
    jclass runtime_exception_class = nullptr;
    jclass unsupportedoperation_exception_class = nullptr;
    jclass illegal_access_exception_class = nullptr;
    jclass illegal_argument_exception_class = nullptr;

};
//

#define LOCAL_ENGINE_JNI_METHOD_START \
    try {

#define LOCAL_ENGINE_JNI_METHOD_END(env, ret) \
    }\
    catch(DB::Exception & e)\
    {\
        local_engine::JniErrorsGlobalState::instance().throwException(env, e);\
        return ret;\
    }\
    catch (std::exception & e)\
    {\
        local_engine::JniErrorsGlobalState::instance().throwException(env, e);\
        return ret;\
    }\
    catch (...)\
    {\
        std::ostringstream ostr;\
        ostr << boost::stacktrace::stacktrace();\
        local_engine::JniErrorsGlobalState::instance().throwRuntimeException(env, "Unknow Exception", ostr.str().c_str());\
        return ret;\
    }
}
