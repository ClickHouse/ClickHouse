set(THRIFT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src")

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

#set(JNI_NATIVE_SOURCES local_engine_jni.cpp)
set(LOCALENGINE_SHARED_LIB ch)

add_subdirectory(proto)

add_headers_and_sources(builder Builder)
add_headers_and_sources(parser Parser)
add_headers_and_sources(storages Storages)
add_headers_and_sources(common Common)
add_headers_and_sources(external External)
add_headers_and_sources(shuffle Shuffle)
add_headers_and_sources(operator Operator)
add_headers_and_sources(jni jni)
add_headers_and_sources(aggregate AggregateFunctions)

include_directories(
        ${JNI_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR}/proto
        ${THRIFT_INCLUDE_DIR}
        ${CMAKE_BINARY_DIR}/contrib/thrift-cmake
        ${ClickHouse_SOURCE_DIR}/utils/local-engine
        ${ClickHouse_SOURCE_DIR}/src
        ${ClickHouse_SOURCE_DIR}/base
)

add_subdirectory(Storages/ch_parquet)
add_subdirectory(Storages/SubstraitSource)

add_library(${LOCALENGINE_SHARED_LIB} SHARED
        ${builder_sources}
        ${parser_sources}
        ${storages_sources}
        ${common_sources}
        ${external_sources}
        ${shuffle_sources}
        ${jni_sources}
        ${substrait_source}
        ${operator_sources}
        ${aggregate_sources}
        local_engine_jni.cpp)


target_compile_options(${LOCALENGINE_SHARED_LIB} PUBLIC -fPIC)

target_link_libraries(${LOCALENGINE_SHARED_LIB} PUBLIC
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        substrait
        loggers
        ch_parquet
        substait_source
)

# if (USE_LIBCXX)
target_compile_options(unwind PRIVATE -fPIC)
target_compile_options(cxxabi PRIVATE -fPIC)
target_compile_options(cxx PRIVATE -fPIC)
# endif()
target_compile_options(_ssl PRIVATE -fPIC)
target_compile_options(_crypto PRIVATE -fPIC)
target_compile_options(_libpq PRIVATE -fPIC)
target_compile_options(_libpqxx PRIVATE -fPIC)
target_compile_options(_lz4 PRIVATE -fPIC)
target_compile_options(_snappy PRIVATE -fPIC)
target_compile_options(_zstd PRIVATE -fPIC)
target_compile_options(_s2 PRIVATE -fPIC)
target_compile_options(_sqlite PRIVATE -fPIC)
target_compile_options(_zlib PRIVATE -fPIC)
target_compile_options(_krb5 PRIVATE -fPIC)
target_compile_options(_libprotobuf PRIVATE -fPIC)
target_compile_options(re2 PRIVATE -fPIC)
target_compile_options(_ldap_r PRIVATE -fPIC)
#target_compile_options(_thrift_static PRIVATE -fPIC)
target_compile_options(_double-conversion PRIVATE -fPIC)
#target_compile_options(arrow_static PRIVATE -fPIC)
#target_compile_options(parquet_static PRIVATE -fPIC)
target_compile_options(_lber PRIVATE -fPIC)
target_compile_options(_poco_foundation PRIVATE -fPIC)
target_compile_options(_fmt PRIVATE -fPIC)
target_compile_options(_cctz PRIVATE -fPIC)
target_compile_options(_poco_xml PRIVATE -fPIC)
target_compile_options(_poco_util PRIVATE -fPIC)
target_compile_options(_poco_json PRIVATE -fPIC)
target_compile_options(_poco_net PRIVATE -fPIC)
target_compile_options(_poco_net_ssl PRIVATE -fPIC)
target_compile_options(common PRIVATE -fPIC)
if (ENABLE_MYSQL)
    target_compile_options(_mariadbclient PRIVATE -fPIC)
    target_compile_options(mysqlxx PRIVATE -fPIC)
endif ()
target_compile_options(_nuraft PRIVATE -fPIC)
target_compile_options(glibc-compatibility PRIVATE -fPIC)
target_compile_options(_poco_foundation_pcre PRIVATE -fPIC)
target_compile_options(_poco_xml_expat PRIVATE -fPIC)
target_compile_options(_poco_crypto PRIVATE -fPIC)
if (ENABLE_KAFKA)
    target_compile_options(_rdkafka PRIVATE -fPIC)
endif ()

target_compile_options(_icuuc PRIVATE -fPIC)
target_compile_options(_icui18n PRIVATE -fPIC)
target_compile_options(_cpuid PRIVATE -fPIC)
target_compile_options(re2_st PRIVATE -fPIC)
target_compile_options(_boost_program_options PRIVATE -fPIC)
target_compile_options(_boost_context PRIVATE -fPIC)
target_compile_options(clickhouse_common_io PRIVATE -fPIC)
target_compile_options(clickhouse_dictionaries_embedded PRIVATE -fPIC)
target_compile_options(clickhouse_common_zookeeper PRIVATE -fPIC)
target_compile_options(clickhouse_parsers PRIVATE -fPIC)
target_compile_options(clickhouse_aggregate_functions PRIVATE -fPIC)
target_compile_options(clickhouse_common_config PRIVATE -fPIC)
target_compile_options(clickhouse_dictionaries PRIVATE -fPIC)
target_compile_options(clickhouse_functions PRIVATE -fPIC)
target_compile_options(clickhouse_common_access PRIVATE -fPIC)
target_compile_options(clickhouse_storages_system PRIVATE -fPIC)
target_compile_options(clickhouse_table_functions PRIVATE -fPIC)
target_compile_options(substrait PRIVATE -fPIC)
target_compile_options(loggers PRIVATE -fPIC)

if (ENABLE_EMBEDDED_COMPILER)
    target_compile_options(LLVMDemangle PRIVATE -fPIC)
    target_compile_options(LLVMSupport PRIVATE -fPIC)
    target_compile_options(LLVMDebugInfoCodeView PRIVATE -fPIC)
    target_compile_options(LLVMMC PRIVATE -fPIC)
    target_compile_options(LLVMMCParser PRIVATE -fPIC)
    target_compile_options(LLVMRemarks PRIVATE -fPIC)
    target_compile_options(LLVMCore PRIVATE -fPIC)
    target_compile_options(LLVMMCParser PRIVATE -fPIC)
    target_compile_options(LLVMBitReader PRIVATE -fPIC)
    target_compile_options(LLVMTextAPI PRIVATE -fPIC)
    target_compile_options(LLVMObject PRIVATE -fPIC)
    target_compile_options(LLVMProfileData PRIVATE -fPIC)
    target_compile_options(LLVMAnalysis PRIVATE -fPIC)
    target_compile_options(LLVMProfileData PRIVATE -fPIC)
    target_compile_options(LLVMTransformUtils PRIVATE -fPIC)
    target_compile_options(LLVMVectorize PRIVATE -fPIC)
    target_compile_options(LLVMInstCombine PRIVATE -fPIC)
    target_compile_options(LLVMBinaryFormat PRIVATE -fPIC)
    target_compile_options(LLVMTarget PRIVATE -fPIC)
    target_compile_options(LLVMBitstreamReader PRIVATE -fPIC)
    target_compile_options(LLVMTarget PRIVATE -fPIC)
    target_compile_options(LLVMAggressiveInstCombine PRIVATE -fPIC)
    target_compile_options(LLVMScalarOpts PRIVATE -fPIC)
    target_compile_options(LLVMInstrumentation PRIVATE -fPIC)
    target_compile_options(LLVMBitWriter PRIVATE -fPIC)
    target_compile_options(LLVMFrontendOpenMP PRIVATE -fPIC)
    target_compile_options(LLVMipo PRIVATE -fPIC)
    target_compile_options(LLVMCodeGen PRIVATE -fPIC)
    target_compile_options(LLVMSelectionDAG PRIVATE -fPIC)
    target_compile_options(LLVMGlobalISel PRIVATE -fPIC)
    target_compile_options(LLVMDebugInfoDWARF PRIVATE -fPIC)
    target_compile_options(_poco_json_pdjson PRIVATE -fPIC)
    target_compile_options(LLVMAsmPrinter PRIVATE -fPIC)
    target_compile_options(LLVMX86Desc PRIVATE -fPIC)
    target_compile_options(LLVMX86CodeGen PRIVATE -fPIC)
    target_compile_options(LLVMRuntimeDyld PRIVATE -fPIC)
    target_compile_options(LLVMMCDisassembler PRIVATE -fPIC)
    target_compile_options(LLVMX86Info PRIVATE -fPIC)
    target_compile_options(LLVMCFGuard PRIVATE -fPIC)
    target_compile_options(LLVMExecutionEngine PRIVATE -fPIC)
endif()

target_compile_options(_roaring PRIVATE -fPIC)
target_compile_options(_yaml_cpp PRIVATE -fPIC)
target_compile_options(dbms PRIVATE -fPIC)
target_compile_options(clickhouse_functions_jsonpath PRIVATE -fPIC)
target_compile_options(_dragonbox_to_chars PRIVATE -fPIC)
target_compile_options(clickhouse_functions_gatherutils PRIVATE -fPIC)
target_compile_options(clickhouse_functions_array PRIVATE -fPIC)
target_compile_options(_hyperscan PRIVATE -fPIC)
target_compile_options(_simdjson PRIVATE -fPIC)
target_compile_options(divide_impl_avx2 PRIVATE -fPIC)
target_compile_options(divide_impl PRIVATE -fPIC)
target_compile_options(_murmurhash PRIVATE -fPIC)
target_compile_options(_fastops PRIVATE -fPIC)
target_compile_options(_base64 PRIVATE -fPIC)
target_compile_options(_base64_avx PRIVATE -fPIC)
target_compile_options(_base64_avx2 PRIVATE -fPIC)
target_compile_options(_base64_ssse3 PRIVATE -fPIC)
target_compile_options(_base64_scalar PRIVATE -fPIC)
target_compile_options(clickhouse_functions_url PRIVATE -fPIC)
target_compile_options(divide_impl_sse2 PRIVATE -fPIC)
target_compile_options(_metrohash PRIVATE -fPIC)
target_compile_options(_consistent_hashing PRIVATE -fPIC)
target_compile_options(_h3 PRIVATE -fPIC)
target_compile_options(_farmhash PRIVATE -fPIC)
target_compile_options(_cityhash PRIVATE -fPIC)
target_compile_options(_liblzma PRIVATE -fPIC)
target_compile_options(_brotli PRIVATE -fPIC)
target_compile_options(_bzip2 PRIVATE -fPIC)
if (ENABLE_NLP)
    target_compile_options(_stemmer PRIVATE -fPIC)
    target_compile_options(_wnb PRIVATE -fPIC)
    target_compile_options(_lemmagen PRIVATE -fPIC)
endif ()

target_compile_options(_aws_s3 PRIVATE -fPIC)
target_compile_options(_azure_sdk PRIVATE -fPIC)
target_compile_options(_curl PRIVATE -fPIC)
if (ENABLE_HDFS)
    target_compile_options(_hdfs3 PRIVATE -fPIC)
endif ()
target_compile_options(_libxml2 PRIVATE -fPIC)
target_compile_options(_gsasl PRIVATE -fPIC)
target_compile_options(_parquet PRIVATE -fPIC)
target_compile_options(_arrow PRIVATE -fPIC)
target_compile_options(_thrift PRIVATE -fPIC)
target_compile_options(_aws_s3_checksums PRIVATE -fPIC)

target_compile_options(absl_str_format_internal PRIVATE -fPIC)
target_compile_options(absl_strings PRIVATE -fPIC)
target_compile_options(absl_raw_logging_internal PRIVATE -fPIC)
target_compile_options(absl_int128 PRIVATE -fPIC)
target_compile_options(absl_raw_hash_set PRIVATE -fPIC)
target_compile_options(absl_raw_hash_set PRIVATE -fPIC)
target_compile_options(absl_base PRIVATE -fPIC)
target_compile_options(absl_spinlock_wait PRIVATE -fPIC)

add_compile_options(-fPIC)
if (ENABLE_JEMALLOC)
    target_compile_options(_jemalloc PRIVATE -fPIC)
endif ()

message(COMPILER_FLAGS ${COMPILER_FLAGS})
message(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
#target_compile_options(zstd PRIVATE -fPIC)

#set(CPACK_PACKAGE_VERSION 0.1.0)
#set(CPACK_GENERATOR "RPM")
#set(CPACK_PACKAGE_NAME "local_engine_jni")
#set(CPACK_PACKAGE_RELEASE 1)
#set(CPACK_CMAKE_GENERATOR Ninja)
#set(CPACK_PACKAGE_CONTACT "neng.liu@kyligence.io")
#set(CPACK_PACKAGE_VENDOR "Kyligence")
#set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
#set(CPACK_RPM_PACKAGE_AUTOREQPROV "no")
#set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
#include(CPack)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()

