#include <stdexcept>
#include <jni/jni_error.h>
#include <jni/jni_common.h>
#include <jni.h>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include <base/logger_useful.h>

namespace local_engine
{
JniErrorsGlobalState & JniErrorsGlobalState::instance()
{
    static JniErrorsGlobalState instance;
    return instance;
}

void JniErrorsGlobalState::destroy(JNIEnv * env)
{
    if (env)
    {
        if (io_exception_class)
        {
            env->DeleteGlobalRef(io_exception_class);
        }
        if (runtime_exception_class)
        {
            env->DeleteGlobalRef(runtime_exception_class);
        }
        if (unsupportedoperation_exception_class)
        {
            env->DeleteGlobalRef(unsupportedoperation_exception_class);
        }
        if (illegal_access_exception_class)
        {
            env->DeleteGlobalRef(illegal_access_exception_class);
        }
        if (illegal_argument_exception_class)
        {
            env->DeleteGlobalRef(illegal_argument_exception_class);
        }
    }
}

void JniErrorsGlobalState::initialize(JNIEnv * env_)
{
    io_exception_class = CreateGlobalExceptionClassReference(env_, "Ljava/io/IOException;");
    runtime_exception_class = CreateGlobalExceptionClassReference(env_, "Ljava/lang/RuntimeException;");
    unsupportedoperation_exception_class = CreateGlobalExceptionClassReference(env_, "Ljava/lang/UnsupportedOperationException;");
    illegal_access_exception_class = CreateGlobalExceptionClassReference(env_, "Ljava/lang/IllegalAccessException;");
    illegal_argument_exception_class = CreateGlobalExceptionClassReference(env_, "Ljava/lang/IllegalArgumentException;");
}

void JniErrorsGlobalState::throwException(JNIEnv * env, const DB::Exception & e)
{
    throwRuntimeException(env, e.message(), e.getStackTraceString());
}

void JniErrorsGlobalState::throwException(JNIEnv * env, const std::exception & e)
{
    throwRuntimeException(env, e.what(), DB::getExceptionStackTraceString(e));
}

void JniErrorsGlobalState::throwException(JNIEnv * env,jclass exception_class, const std::string & message, const std::string & stack_trace)
{
    if (exception_class)
    {
        std::string error_msg = message + "\n" + stack_trace;
        env->ThrowNew(exception_class, error_msg.c_str());
    }
    else
    {
        // This will cause a coredump
        throw std::runtime_error("Not found java runtime exception class");
    }

}

void JniErrorsGlobalState::throwRuntimeException(JNIEnv * env,const std::string & message, const std::string & stack_trace)
{
    throwException(env, runtime_exception_class, message, stack_trace);
}


}
