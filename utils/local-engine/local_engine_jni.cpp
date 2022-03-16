#include <string>
#include <jni.h>
#include "jni_common.h"
#include <Parser/SerializedPlanParser.h>
#include <DataTypes/DataTypeNullable.h>
#include <numeric>

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
static jclass ch_column_batch_class;

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

    ch_column_batch_class = CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/CHColumnVector");

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

// Columnar Iterator
jboolean Java_com_intel_oap_vectorized_BatchIterator_nativeHasNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    return executor->hasNext();
}

jobject Java_com_intel_oap_vectorized_BatchIterator_nativeNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    dbms::LocalExecutor * executor = reinterpret_cast<dbms::LocalExecutor *>(executor_address);
    Block & columnBatch = executor->nextColumnar();
    return 0;
}

void Java_com_intel_oap_vectorized_BatchIterator_nativeClose(JNIEnv * env, jobject obj, jlong executor_address)
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

// CHColumnBatch

jlong getCHColumnVectorBlockAddress(JNIEnv * env, jobject obj)
{
    jfieldID fid = env->GetFieldID(ch_column_batch_class,"blockAddress","L");
    return env->GetLongField(obj,fid);
}

jint getCHColumnVectorPosition(JNIEnv * env, jobject obj)
{
    jfieldID fid = env->GetFieldID(ch_column_batch_class,"blockAddress","L");
    return env->GetIntField(obj,fid);
}

ColumnWithTypeAndName inline getColumnFromColumnVector(JNIEnv * env, jobject obj)
{
    Block * block = reinterpret_cast<Block *>(getCHColumnVectorBlockAddress(env, obj));
    int position = getCHColumnVectorPosition(env, obj);
    return block->getByPosition(position);
}


jboolean Java_com_intel_oap_vectorized_CHColumnVector_hasNull(JNIEnv * env, jobject obj)
{
    Block * block = reinterpret_cast<Block *>(getCHColumnVectorBlockAddress(env, obj));
    int position = getCHColumnVectorPosition(env, obj);
    auto col = block->getByPosition(position);
    if (!col.column->isNullable())
    {
        return false;
    }
    else
    {
        auto * nullable = checkAndGetColumn<ColumnNullable>(*col.column);
        size_t num_nulls = std::accumulate(nullable->getNullMapData().begin(), nullable->getNullMapData().end(), 0);
        return num_nulls < block->rows();
    }
}

jint Java_com_intel_oap_vectorized_CHColumnVector_numNulls(JNIEnv * env, jobject obj)
{
    auto col = getColumnFromColumnVector(env, obj);
    if (!col.column->isNullable())
    {
        return 0;
    }
    else
    {
        auto * nullable = checkAndGetColumn<ColumnNullable>(*col.column);
        return std::accumulate(nullable->getNullMapData().begin(), nullable->getNullMapData().end(), 0);
    }
}


jboolean Java_com_intel_oap_vectorized_CHColumnVector_isNullAt(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return col.column->isNullAt(row_id);
}

jboolean Java_com_intel_oap_vectorized_CHColumnVector_getBoolean(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return col.column->getBool(row_id);
}

jbyte Java_com_intel_oap_vectorized_CHColumnVector_getByte(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return reinterpret_cast<const jbyte*>(col.column->getDataAt(row_id).data)[0];}

jshort Java_com_intel_oap_vectorized_CHColumnVector_getShort(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return reinterpret_cast<const jshort*>(col.column->getDataAt(row_id).data)[0];
}

jint Java_com_intel_oap_vectorized_CHColumnVector_getInt(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return reinterpret_cast<const jint*>(col.column->getDataAt(row_id).data)[0];
}

jlong Java_com_intel_oap_vectorized_CHColumnVector_getLong(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return col.column->getInt(row_id);
}

jfloat Java_com_intel_oap_vectorized_CHColumnVector_getFloat(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return col.column->getFloat32(row_id);
}

jdouble Java_com_intel_oap_vectorized_CHColumnVector_getDouble(JNIEnv * env, jobject obj, jint row_id)
{
    auto col = getColumnFromColumnVector(env, obj);
    return col.column->getFloat64(row_id);
}

// native block
jint Java_com_intel_oap_vectorized_CHNativeBlock_nativeNumRows(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    return block->rows();
}

jint Java_com_intel_oap_vectorized_CHNativeBlock_nativeNumColumns(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    return block->columns();
}

jstring Java_com_intel_oap_vectorized_CHNativeBlock_nativeColumnType(JNIEnv * env, jobject obj, jlong block_address, jint position)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    WhichDataType which(block->getByPosition(position).type);
    std::string type;
    if (which.isNullable())
    {
        const auto * nullable = checkAndGetDataType<DataTypeNullable>(block->getByPosition(position).type.get());
        which = WhichDataType(nullable->getNestedType());
    }
    if (which.isDate())
    {
        type = "Date";
    }
    else if (which.isFloat32())
    {
        type = "Float";
    }
    else if (which.isFloat64())
    {
        type = "Double";
    }
    else if (which.isInt32())
    {
        type = "Integer";
    }
    else if (which.isInt64())
    {
        type = "Long";
    }
    else if (which.isInt8())
    {
        type = "Byte";
    }
    else if (which.isInt16())
    {
        type = "Short";
    }
    else if (which.isString())
    {
        type = "String";
    }
    else
    {
        throw "unsupported datatype";
    }

    return charTojstring(env, type.c_str());
}

jlong Java_com_intel_oap_vectorized_CHNativeBlock_nativeTotalBytes(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    return block->bytes();
}

#ifdef __cplusplus
}
#endif
