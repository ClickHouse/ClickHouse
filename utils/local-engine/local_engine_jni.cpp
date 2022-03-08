#include <string>
#include <jni.h>
#include "jni_common.h"
#include <Parser/SerializedPlanParser.h>





bool inside_main = true;
#ifdef __cplusplus
extern "C" {
#endif

extern void registerAllFunctions();
extern void init();
extern char * createExecutor(std::string plan_string);

namespace dbms
{
    class LocalExecutor;
}

static jclass spark_row_info_class;
static jmethodID spark_row_info_constructor;

jint JNI_OnLoad(JavaVM * vm, void * reserved)
{
    JNIEnv * env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK)
    {
        return JNI_ERR;
    }
    io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
    runtime_exception_class = CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");
    unsupportedoperation_exception_class = CreateGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
    illegal_access_exception_class = CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
    illegal_argument_exception_class = CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

    spark_row_info_class = CreateGlobalClassReference(env, "Lcom/intel/oap/row/SparkRowInfo;");
    spark_row_info_constructor = env->GetMethodID(spark_row_info_class, "<init>", "([J[JJJ)V");
    return JNI_VERSION_1_8;
}

void JNI_OnUnload(JavaVM * vm, void * reserved)
{
    std::cerr << "JNI_OnUnload" << std::endl;
    JNIEnv * env;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);

    env->DeleteGlobalRef(io_exception_class);
    env->DeleteGlobalRef(runtime_exception_class);
    env->DeleteGlobalRef(unsupportedoperation_exception_class);
    env->DeleteGlobalRef(illegal_access_exception_class);
    env->DeleteGlobalRef(illegal_argument_exception_class);
}
//static SharedContextHolder shared_context;

void Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(JNIEnv *, jobject)
{
    init();
}

jlong Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithRowIterator(JNIEnv * env, jobject obj, jbyteArray plan)
{
    jsize plan_size = env->GetArrayLength(plan);
    jbyte * plan_address = env->GetByteArrayElements(plan, nullptr);
    std::string plan_string;
    plan_string.assign(reinterpret_cast<const char *>(plan_address), plan_size);
    auto * executor = createExecutor( plan_string);
    return reinterpret_cast<jlong>(executor);
}

jboolean Java_com_intel_oap_row_RowIterator_nativeHasNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    return executor->hasNext();
//    return false;
}

jobject Java_com_intel_oap_row_RowIterator_nativeNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    local_engine::SparkRowInfoPtr spark_row_info = executor->next();

    auto *offsets_arr = env->NewLongArray(spark_row_info->getNumRows());
    const auto *offsets_src = reinterpret_cast<const jlong*>(spark_row_info->getOffsets().data());
    env->SetLongArrayRegion(offsets_arr, 0, spark_row_info->getNumRows(), offsets_src);
    auto *lengths_arr = env->NewLongArray(spark_row_info->getNumRows());
    const auto *lengths_src = reinterpret_cast<const jlong*>(spark_row_info->getLengths().data());
    env->SetLongArrayRegion(lengths_arr, 0, spark_row_info->getNumRows(), lengths_src);
    int64_t address = reinterpret_cast<int64_t>(spark_row_info->getBufferAddress());
    int64_t column_number = reinterpret_cast<int64_t>(spark_row_info->getNumCols());

    jobject spark_row_info_object = env->NewObject(
        spark_row_info_class, spark_row_info_constructor,
        offsets_arr, lengths_arr, address, column_number);

    return spark_row_info_object;
}

void Java_com_intel_oap_row_RowIterator_nativeClose(JNIEnv * env, jobject obj, jlong executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    delete executor;
}

void Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(JNIEnv * env, jobject obj, jstring dir)
{
}

void Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(JNIEnv * env, jobject obj, jint batch_size)
{
}

void Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(JNIEnv * env, jobject obj, jboolean setMetricsTime)
{
}

#ifdef __cplusplus
}
#endif
