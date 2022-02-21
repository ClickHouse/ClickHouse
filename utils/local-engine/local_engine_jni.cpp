#include <iostream>
#include <string>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/Sinks/NullSink.h>
#include "include/com_intel_oap_row_RowIterator.h"
#include "include/com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnsNumber.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>

#include <fstream>
#include <jni.h>
#include <Processors/Pipe.h>
#include "jni_common.h"

using namespace DB;

void registerAllFunctions()
{
    registerFunctions();
    registerAggregateFunctions();
}

bool inside_main = false;
#ifdef __cplusplus
extern "C" {
#endif

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
void Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(JNIEnv *, jobject)
{
    registerAllFunctions();
}

jlong Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithRowIterator(JNIEnv * env, jobject obj, jbyteArray plan)
{
    jsize plan_size = env->GetArrayLength(plan);
    jbyte * plan_address = env->GetByteArrayElements(plan, nullptr);
    std::string plan_string;
    plan_string.assign(reinterpret_cast<const char *>(plan_address), plan_size);
    dbms::SerializedPlanParser parser(dbms::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(plan_string);
    dbms::LocalExecutor * executor = new dbms::LocalExecutor();
    executor->execute(std::move(query_plan));
    return reinterpret_cast<jlong>(executor);
}

jboolean Java_com_intel_oap_row_RowIterator_nativeHasNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    return executor->hasNext();
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
