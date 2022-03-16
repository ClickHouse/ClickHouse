#pragma once

static jclass io_exception_class;
static jclass runtime_exception_class;
static jclass unsupportedoperation_exception_class;
static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
    jclass local_class = env->FindClass(class_name);
    jclass global_class = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    if (global_class == nullptr) {
        std::string error_message =
            "Unable to createGlobalClassReference for" + std::string(class_name);
        env->ThrowNew(illegal_access_exception_class, error_message.c_str());
    }
    return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
    jmethodID ret = env->GetMethodID(this_class, name, sig);
    if (ret == nullptr) {
        std::string error_message = "Unable to find method " + std::string(name) +
            " within signature" + std::string(sig);
        env->ThrowNew(illegal_access_exception_class, error_message.c_str());
    }

    return ret;
}

jmethodID GetStaticMethodID(JNIEnv* env, jclass this_class, const char* name,
                            const char* sig) {
    jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
    if (ret == nullptr) {
        std::string error_message = "Unable to find static method " + std::string(name) +
            " within signature" + std::string(sig);
        env->ThrowNew(illegal_access_exception_class, error_message.c_str());
    }
    return ret;
}

jstring charTojstring(JNIEnv* env, const char* pat) {
    jclass strClass = (env)->FindClass("Ljava/lang/String;");
    jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
    jbyteArray bytes = (env)->NewByteArray(strlen(pat));
    (env)->SetByteArrayRegion(bytes, 0, strlen(pat), (jbyte*) pat);
    jstring encoding = (env)->NewStringUTF("UTF-8");
    return (jstring) (env)->NewObject(strClass, ctorID, bytes, encoding);
}