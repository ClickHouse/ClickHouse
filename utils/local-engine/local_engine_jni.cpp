#include <numeric>
#include <regex>
#include <string>
#include <jni.h>
#include <DataTypes/DataTypeNullable.h>
#include <Operator/BlockCoalesceOperator.h>
#include <Parser/SerializedPlanParser.h>
#include <Shuffle/ShuffleReader.h>
#include <Shuffle/ShuffleSplitter.h>
#include <Common/ExceptionUtils.h>
#include "jni_common.h"

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

static jclass split_result_class;
static jmethodID split_result_constructor;
static JavaVM * global_vm = nullptr;

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

    spark_row_info_class = CreateGlobalClassReference(env, "Lio/glutenproject/row/SparkRowInfo;");
    spark_row_info_constructor = env->GetMethodID(spark_row_info_class, "<init>", "([J[JJJ)V");

    split_result_class = CreateGlobalClassReference(env, "Lio/glutenproject/vectorized/SplitResult;");
    split_result_constructor = GetMethodID(env, split_result_class, "<init>", "(JJJJJJ[J[J)V");

    ch_column_batch_class = CreateGlobalClassReference(env, "Lio/glutenproject/vectorized/CHColumnVector;");
    local_engine::ShuffleReader::input_stream_class = CreateGlobalClassReference(env, "Ljava/io/InputStream;");
    local_engine::ShuffleReader::input_stream_read = env->GetMethodID(local_engine::ShuffleReader::input_stream_class, "read", "([B)I");

    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class
        = CreateGlobalClassReference(env, "Lio/glutenproject/execution/ColumnarNativeIterator;");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_hasNext
        = GetMethodID(env, local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class, "hasNext", "()Z");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_next
        = GetMethodID(env, local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class, "next", "()[B");
    global_vm = vm;
    return JNI_VERSION_1_8;
}

void JNI_OnUnload(JavaVM * vm, void * reserved)
{
    JNIEnv * env;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);

    env->DeleteGlobalRef(io_exception_class);
    env->DeleteGlobalRef(runtime_exception_class);
    env->DeleteGlobalRef(unsupportedoperation_exception_class);
    env->DeleteGlobalRef(illegal_access_exception_class);
    env->DeleteGlobalRef(illegal_argument_exception_class);
    env->DeleteGlobalRef(split_result_class);
    env->DeleteGlobalRef(local_engine::ShuffleReader::input_stream_class);
    env->DeleteGlobalRef(local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class);
}
//static SharedContextHolder shared_context;

void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(JNIEnv *, jobject)
{
    try
    {
        init();
    }
    catch (const DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jlong Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithRowIterator(
    JNIEnv * env, jobject obj, jbyteArray plan)
{
    try
    {
        jsize plan_size = env->GetArrayLength(plan);
        jbyte * plan_address = env->GetByteArrayElements(plan, nullptr);
        std::string plan_string;
        plan_string.assign(reinterpret_cast<const char *>(plan_address), plan_size);
        auto * executor = createExecutor(plan_string);
        env->ReleaseByteArrayElements(plan, plan_address, JNI_ABORT);
        return reinterpret_cast<jlong>(executor);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jlong Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv * env, jobject obj, jlong, jbyteArray plan, jobjectArray iter_arr)
{
    try
    {
        auto context = Context::createCopy(local_engine::SerializedPlanParser::global_context);
        local_engine::SerializedPlanParser parser(context);
        parser.setJavaVM(global_vm);
        jsize iter_num = env->GetArrayLength(iter_arr);
        for (jsize i = 0; i < iter_num; i++)
        {
            jobject iter = env->GetObjectArrayElement(iter_arr, i);
            iter = env->NewGlobalRef(iter);
            parser.addInputIter(iter);
        }
        jsize plan_size = env->GetArrayLength(plan);
        jbyte * plan_address = env->GetByteArrayElements(plan, nullptr);
        std::string plan_string;
        plan_string.assign(reinterpret_cast<const char *>(plan_address), plan_size);
        auto query_plan = parser.parse(plan_string);
        local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context);
        executor->execute(std::move(query_plan));
        env->ReleaseByteArrayElements(plan, plan_address, JNI_ABORT);
        return reinterpret_cast<jlong>(executor);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

jboolean Java_io_glutenproject_row_RowIterator_nativeHasNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    try
    {
        local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
        return executor->hasNext();
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jobject Java_io_glutenproject_row_RowIterator_nativeNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    try
    {
        local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
        local_engine::SparkRowInfoPtr spark_row_info = executor->next();

        auto * offsets_arr = env->NewLongArray(spark_row_info->getNumRows());
        const auto * offsets_src = reinterpret_cast<const jlong *>(spark_row_info->getOffsets().data());
        env->SetLongArrayRegion(offsets_arr, 0, spark_row_info->getNumRows(), offsets_src);
        auto * lengths_arr = env->NewLongArray(spark_row_info->getNumRows());
        const auto * lengths_src = reinterpret_cast<const jlong *>(spark_row_info->getLengths().data());
        env->SetLongArrayRegion(lengths_arr, 0, spark_row_info->getNumRows(), lengths_src);
        int64_t address = reinterpret_cast<int64_t>(spark_row_info->getBufferAddress());
        int64_t column_number = reinterpret_cast<int64_t>(spark_row_info->getNumCols());

        jobject spark_row_info_object
            = env->NewObject(spark_row_info_class, spark_row_info_constructor, offsets_arr, lengths_arr, address, column_number);

        return spark_row_info_object;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

void Java_io_glutenproject_row_RowIterator_nativeClose(JNIEnv * env, jobject obj, jlong executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    delete executor;
}

// Columnar Iterator
jboolean Java_io_glutenproject_vectorized_BatchIterator_nativeHasNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    try
    {
        local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
        return executor->hasNext();
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jlong Java_io_glutenproject_vectorized_BatchIterator_nativeCHNext(JNIEnv * env, jobject obj, jlong executor_address)
{
    try
    {
        local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
        Block * column_batch = executor->nextColumnar();
        return reinterpret_cast<Int64>(column_batch);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

void Java_io_glutenproject_vectorized_BatchIterator_nativeClose(JNIEnv * env, jobject obj, jlong executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    delete executor;
}


void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(JNIEnv * env, jobject obj, jstring dir)
{
}

void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(JNIEnv * env, jobject obj, jint batch_size)
{
}

void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(JNIEnv * env, jobject obj, jboolean setMetricsTime)
{
}

ColumnWithTypeAndName inline getColumnFromColumnVector(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    try
    {
        Block * block = reinterpret_cast<Block *>(block_address);
        return block->getByPosition(column_position);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}


jboolean Java_io_glutenproject_vectorized_CHColumnVector_nativeHasNull(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    try
    {
        Block * block = reinterpret_cast<Block *>(block_address);
        auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
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
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jint Java_io_glutenproject_vectorized_CHColumnVector_nativeNumNulls(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    try
    {
        auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
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
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}


jboolean Java_io_glutenproject_vectorized_CHColumnVector_nativeIsNullAt(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->isNullAt(row_id);
}

jboolean Java_io_glutenproject_vectorized_CHColumnVector_nativeGetBoolean(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->getBool(row_id);
}

jbyte Java_io_glutenproject_vectorized_CHColumnVector_nativeGetByte(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return reinterpret_cast<const jbyte *>(col.column->getDataAt(row_id).data)[0];
}

jshort Java_io_glutenproject_vectorized_CHColumnVector_nativeGetShort(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return reinterpret_cast<const jshort *>(col.column->getDataAt(row_id).data)[0];
}

jint Java_io_glutenproject_vectorized_CHColumnVector_nativeGetInt(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    if (col.type->getTypeId() == TypeIndex::Date)
    {
        return col.column->getUInt(row_id);
    }
    else
    {
        return col.column->getInt(row_id);
    }
}

jlong Java_io_glutenproject_vectorized_CHColumnVector_nativeGetLong(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->getInt(row_id);
}

jfloat Java_io_glutenproject_vectorized_CHColumnVector_nativeGetFloat(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->getFloat32(row_id);
}

jdouble Java_io_glutenproject_vectorized_CHColumnVector_nativeGetDouble(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->getFloat64(row_id);
}

jstring Java_io_glutenproject_vectorized_CHColumnVector_nativeGetString(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    const ColumnString * col = checkAndGetColumn<ColumnString>(*getColumnFromColumnVector(env, obj, block_address, column_position).column);
    auto result = col->getDataAt(row_id);
    return charTojstring(env, result.toString().c_str());
}

// native block
void Java_io_glutenproject_vectorized_CHNativeBlock_nativeClose(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    block->clear();
    delete block;
}

jint Java_io_glutenproject_vectorized_CHNativeBlock_nativeNumRows(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    return block->rows();
}

jint Java_io_glutenproject_vectorized_CHNativeBlock_nativeNumColumns(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    return block->columns();
}

jstring Java_io_glutenproject_vectorized_CHNativeBlock_nativeColumnType(JNIEnv * env, jobject obj, jlong block_address, jint position)
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
    else if (which.isUInt64())
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
    else if (which.isAggregateFunction())
    {
        type = "Binary";
    }
    else
    {
        auto type_name = std::string(block->getByPosition(position).type->getFamilyName());
        auto col_name = block->getByPosition(position).name;
        LOG_ERROR(&Poco::Logger::get("jni"), "column {}, unsupported datatype {}", col_name, type_name);
        throw std::runtime_error("unsupported datatype " + type_name);
    }

    return charTojstring(env, type.c_str());
}

jlong Java_io_glutenproject_vectorized_CHNativeBlock_nativeTotalBytes(JNIEnv * env, jobject obj, jlong block_address)
{
    Block * block = reinterpret_cast<Block *>(block_address);
    return block->bytes();
}

jlong Java_io_glutenproject_vectorized_CHStreamReader_createNativeShuffleReader(JNIEnv * env, jclass clazz, jobject input_stream)
{
    try
    {
        auto input = env->NewGlobalRef(input_stream);
        auto read_buffer = std::make_unique<local_engine::ReadBufferFromJavaInputStream>(input);
        auto * shuffle_reader = new local_engine::ShuffleReader(std::move(read_buffer), true);
        return reinterpret_cast<jlong>(shuffle_reader);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jlong Java_io_glutenproject_vectorized_CHStreamReader_nativeNext(JNIEnv * env, jobject obj, jlong shuffle_reader)
{
    try
    {
        local_engine::ShuffleReader::env = env;
        local_engine::ShuffleReader * reader = reinterpret_cast<local_engine::ShuffleReader *>(shuffle_reader);
        Block * block = reader->read();
        local_engine::ShuffleReader::env = nullptr;
        return reinterpret_cast<jlong>(block);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}


void Java_io_glutenproject_vectorized_CHStreamReader_nativeClose(JNIEnv * env, jobject obj, jlong shuffle_reader)
{
    local_engine::ShuffleReader::env = env;
    local_engine::ShuffleReader * reader = reinterpret_cast<local_engine::ShuffleReader *>(shuffle_reader);
    delete reader;
    local_engine::ShuffleReader::env = nullptr;
}

// CHCoalesceOperator

jlong Java_io_glutenproject_vectorized_CHCoalesceOperator_createNativeOperator(JNIEnv * env, jobject obj, jint buf_size)
{
    try
    {
        local_engine::BlockCoalesceOperator * instance = new local_engine::BlockCoalesceOperator(buf_size);
        return reinterpret_cast<jlong>(instance);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

void Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeMergeBlock(
    JNIEnv * env, jobject obj, jlong instance_address, jlong block_address)
{
    try
    {
        local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
        DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
        auto new_block = DB::Block(*block);
        instance->mergeBlock(new_block);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jboolean Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeIsFull(JNIEnv * env, jobject obj, jlong instance_address)
{
    try
    {
        local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
        bool full = instance->isFull();
        return full ? JNI_TRUE : JNI_FALSE;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

jlong Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeRelease(JNIEnv * env, jobject obj, jlong instance_address)
{
    try
    {
        local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
        auto block = instance->releaseBlock();
        DB::Block * new_block = new DB::Block();
        new_block->swap(block);
        long address = reinterpret_cast<jlong>(new_block);
        return address;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }

}

void Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeClose(JNIEnv * env, jobject obj, jlong instance_address)
{
        local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
        delete instance;
}

std::string jstring2string(JNIEnv * env, jstring jStr)
{
    try
    {
        if (!jStr)
            return "";

        const jclass stringClass = env->GetObjectClass(jStr);
        const jmethodID getBytes = env->GetMethodID(stringClass, "getBytes", "(Ljava/lang/String;)[B");
        const jbyteArray stringJbytes = static_cast<jbyteArray>(env->CallObjectMethod(jStr, getBytes, env->NewStringUTF("UTF-8")));

        size_t length = static_cast<size_t>(env->GetArrayLength(stringJbytes));
        jbyte * pBytes = env->GetByteArrayElements(stringJbytes, nullptr);

        std::string ret = std::string(reinterpret_cast<char *>(pBytes), length);
        env->ReleaseByteArrayElements(stringJbytes, pBytes, JNI_ABORT);

        env->DeleteLocalRef(stringJbytes);
        env->DeleteLocalRef(stringClass);
        return ret;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

std::vector<std::string> stringSplit(const std::string & str, char delim)
{
    try
    {
        std::string s;
        s.append(1, delim);
        std::regex reg(s);
        std::vector<std::string> elems(std::sregex_token_iterator(str.begin(), str.end(), reg, -1), std::sregex_token_iterator());
        return elems;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}


// Splitter Jni Wrapper
jlong Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_nativeMake(
    JNIEnv * env,
    jobject,
    jstring short_name,
    jint num_partitions,
    jbyteArray expr_list,
    jlong map_id,
    jint buffer_size,
    jstring codec,
    jstring data_file,
    jstring local_dirs)
{
    try
    {
        std::vector<std::string> expr_vec;
        if (expr_list != nullptr)
        {
            int len = env->GetArrayLength(expr_list);
            auto * str = reinterpret_cast<jbyte *>(new char[len]);
            memset(str, 0, len);
            env->GetByteArrayRegion(expr_list, 0, len, str);
            std::string exprs(str, str + len);
            delete[] str;
            for (const auto & expr : stringSplit(exprs, ','))
            {
                expr_vec.emplace_back(expr);
            }
        }
        local_engine::SplitOptions options{
            .buffer_size = static_cast<size_t>(buffer_size),
            .data_file = jstring2string(env, data_file),
            .local_tmp_dir = jstring2string(env, local_dirs),
            .map_id = static_cast<int>(map_id),
            .partition_nums = static_cast<size_t>(num_partitions),
            .exprs = expr_vec,
            .compress_method = jstring2string(env, codec)};
        local_engine::SplitterHolder * splitter
            = new local_engine::SplitterHolder{.splitter = local_engine::ShuffleSplitter::create(jstring2string(env, short_name), options)};
        return reinterpret_cast<jlong>(splitter);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

void Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_split(JNIEnv *, jobject, jlong splitterId, jint, jlong block)
{
    try
    {
        local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
        Block * data = reinterpret_cast<Block *>(block);
        splitter->splitter->split(*data);
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

jobject Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_stop(JNIEnv * env, jobject, jlong splitterId)
{
    try
    {
        local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
        auto result = splitter->splitter->stop();
        const auto & partition_lengths = result.partition_length;
        auto partition_length_arr = env->NewLongArray(partition_lengths.size());
        auto src = reinterpret_cast<const jlong *>(partition_lengths.data());
        env->SetLongArrayRegion(partition_length_arr, 0, partition_lengths.size(), src);

        const auto & raw_partition_lengths = result.raw_partition_length;
        auto raw_partition_length_arr = env->NewLongArray(raw_partition_lengths.size());
        auto raw_src = reinterpret_cast<const jlong *>(raw_partition_lengths.data());
        env->SetLongArrayRegion(raw_partition_length_arr, 0, raw_partition_lengths.size(), raw_src);

        jobject split_result = env->NewObject(
            split_result_class,
            split_result_constructor,
            result.total_compute_pid_time,
            result.total_write_time,
            result.total_spill_time,
            0,
            result.total_bytes_written,
            result.total_bytes_written,
            partition_length_arr,
            raw_partition_length_arr);

        return split_result;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}
void Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_close(JNIEnv *, jobject, jlong splitterId)
{
    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    delete splitter;
}
#ifdef __cplusplus
}
#endif
