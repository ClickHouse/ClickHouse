set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
set(CLICKHOUSE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/src")
set(USE_INTERNAL_PARQUET_LIBRARY 1)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

cmake_minimum_required(VERSION 3.11)
if(CMAKE_VERSION VERSION_LESS 3.11)
    message(FATAL_ERROR "Building local engine JNI bindings requires CMake version >= 3.11")
endif()
include (${ClickHouse_SOURCE_DIR}/cmake/warnings.cmake)
include_directories("${ClickHouse_SOURCE_DIR}/programs")

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)
message("JNI_INCLUDE_DIRS ${JNI_INCLUDE_DIRS}")
include_directories(${JNI_INCLUDE_DIRS})

set(JNI_NATIVE_SOURCES
        local_engine_jni.cpp)
set (CLICKHOUSE_SERVER_LINK
        PRIVATE
#        dbms
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_dictionaries
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        substrait

        )

function(add_cxx_compile_options option)
    add_compile_options("$<$<STREQUAL:$<TARGET_PROPERTY:LINKER_LANGUAGE>,CXX>:${option}>")
endfunction()
add_cxx_compile_options(-Wzero-as-null-pointer-constant)
add_subdirectory(proto)
add_subdirectory(Builder)
add_headers_and_sources(builder Builder)
add_headers_and_sources(parser Parser)
add_headers_and_sources(storages Storages)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/proto)
include_directories(${ClickHouse_SOURCE_DIR}/utils/local-engine)


include_directories(
        ${ARROW_INCLUDE_DIR}
        )

set(LOCALENGINE_SHARED_LIB local_engine_jni)

add_library(${LOCALENGINE_SHARED_LIB} SHARED
        ${JNI_NATIVE_SOURCES}
        ${builder_headers}
        ${builder_sources}
        ${parser_headers}
        ${parser_sources}
        ${storages_sources}
        ${ClickHouse_SOURCE_DIR}/programs/local/LocalServer.cpp
        )
target_include_directories(${LOCALENGINE_SHARED_LIB} PUBLIC ${ClickHouse_SOURCE_DIR}/src ${ClickHouse_SOURCE_DIR}/base)
target_link_libraries(${LOCALENGINE_SHARED_LIB} ${CLICKHOUSE_SERVER_LINK} loggers)

install(TARGETS ${LOCALENGINE_SHARED_LIB}
        LIBRARY DESTINATION lib
        )

install( FILES
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/AggregateFunctions/libclickhouse_aggregate_functions.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/libclickhouse_functions.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Storages/System/libclickhouse_storages_system.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../utils/local-engine/proto/libsubstrait.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/base64-cmake/libbase64.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libfarmhash/libfarmhash.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/fastops-cmake/libfastops.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Dictionaries/libclickhouse_dictionaries.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/consistent-hashing/libconsistent-hashing.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libmetrohash/libmetrohash.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/murmurhash/libmurmurhash.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libdbms.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_access.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_core.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_core_mysql.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_compression.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_datastreams.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_datatypes.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_datatypes_serializations.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_databases.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_databases_mysql.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_disks.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_interpreters.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_interpreters_mysql.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_interpreters_clusterproxy.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_interpreters_jit.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_columns.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_storages.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_storages_mysql.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_storages_distributed.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_storages_mergetree.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_storages_liveview.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_client.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_bridge.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_server.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_server_http.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_formats.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_executors.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_formats.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_formats_impl.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_transforms.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_sources.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_merges.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_merges_algorithms.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_queryplan.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_processors_queryplan_optimizations.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_coordination.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Dictionaries/Embedded/libclickhouse_dictionaries_embedded.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Parsers/libclickhouse_parsers.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/nuraft-cmake/libnuraft.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../base/mysqlxx/libmysqlxx.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Server/grpc_protos/libclickhouse_grpc_protos.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libgrpc++.so.1
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libgrpc++.so.1.33.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libgrpc.so.13
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libgrpc.so.13.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/hash/libabsl_hash.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/types/libabsl_bad_variant_access.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/hash/libabsl_city.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/container/libabsl_raw_hash_set.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/container/libabsl_hashtablez_sampler.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_exponential_biased.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/third_party/cares/cares/lib/libcares.so.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/third_party/cares/cares/lib/libcares.so.2.3.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/status/libabsl_status.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/types/libabsl_bad_optional_access.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/strings/libabsl_cord.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libaddress_sorting.so.13
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libaddress_sorting.so.13.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libupb.so.13
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libupb.so.13.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libgpr.so.13
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/libgpr.so.13.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/synchronization/libabsl_synchronization.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/debugging/libabsl_stacktrace.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/debugging/libabsl_symbolize.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/debugging/libabsl_debugging_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/debugging/libabsl_demangle_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/synchronization/libabsl_graphcycles_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/time/libabsl_time.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/time/libabsl_civil_time.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/time/libabsl_time_zone.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_malloc_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/strings/libabsl_str_format_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/strings/libabsl_strings.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/strings/libabsl_strings_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/numeric/libabsl_int128.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_throw_delegate.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_base.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_raw_logging_internal.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_dynamic_annotations.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_log_severity.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/abseil-cpp/absl/base/libabsl_spinlock_wait.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/s2geometry-cmake/libs2.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/amqpcpp-cmake/libamqp-cpp.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/sqlite-cmake/libsqlite.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../libcassandra.so.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../libcassandra.so.2.15.3
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libuv/libuv.so.1
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libuv/libuv.so.1.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libpqxx-cmake/liblibpqxx.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libpq-cmake/liblibpq.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/arrow-cmake/libarrow_shared.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/lz4-cmake/liblz4.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/snappy/libsnappy.so.1
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/snappy/libsnappy.so.1.1.7
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/libclickhouse_common_io.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/double-conversion-cmake/libdouble-conversion.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/dragonbox-cmake/libdragonbox_to_chars.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/re2/libre2.so.9
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/re2/libre2.so.9.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/re2_st/libre2_st.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_program_options.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/croaring-cmake/libroaring.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/zstd-cmake/libzstd.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/xz/liblzma.so.5
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/xz/liblzma.so.5.3.1
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/aws-s3-cmake/libaws_s3.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../base/common/libcommon.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/cityhash102/libcityhash.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_system.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Net/SSL/lib_poco_net_ssl.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Net/lib_poco_net.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Crypto/lib_poco_crypto.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boringssl-cmake/libssl.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boringssl-cmake/libcrypto.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Util/lib_poco_util.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/JSON/lib_poco_json.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/JSON/lib_poco_json_pdjson.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/XML/lib_poco_xml.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Foundation/lib_poco_foundation.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/zlib-ng/libz.so.1
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/zlib-ng/libz.so.1.2.11.zlib-ng
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/XML/lib_poco_xml_expat.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/replxx-cmake/libreplxx.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/cctz-cmake/libcctz.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/fmtlib-cmake/libfmt.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/protobuf/libprotobuf.so.3.13.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libcxx-cmake/libcxx.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libcxxabi-cmake/libcxxabi.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libunwind-cmake/libunwind.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/divide/libdivide_impl.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/h3-cmake/libh3.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/simdjson-cmake/libsimdjson.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/URL/libclickhouse_functions_url.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/array/libclickhouse_functions_array.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/JSONPath/libclickhouse_functions_jsonpath.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/divide/libdivide_impl_sse2.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/divide/libdivide_impl_avx2.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/hyperscan-cmake/libhyperscan.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Functions/GatherUtils/libclickhouse_functions_gatherutils.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/icu-cmake/libicui18n.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/icu-cmake/libicuuc.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/icu-cmake/libicudata.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_filesystem.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Common/ZooKeeper/libclickhouse_common_zookeeper.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Common/StringUtils/libstring_utils.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Data/ODBC/lib_poco_data_odbc.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Redis/lib_poco_redis.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/MongoDB/lib_poco_mongodb.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Data/lib_poco_data.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/unixodbc-cmake/libunixodbc.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/cppkafka-cmake/libcppkafka.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/librdkafka-cmake/librdkafka.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/cyrus-sasl-cmake/libsasl2.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../src/Common/Config/libclickhouse_common_config.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/capnproto-cmake/libcapnpc.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/arrow-cmake/libparquet_shared.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/avro-cmake/libavrocpp.so.1.10.0-SNAPSHOT.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libhdfs3-cmake/libhdfs3.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/capnproto-cmake/libcapnp.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/capnproto-cmake/libkj.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/krb5-cmake/libkrb5.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_context.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_coroutine.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/flatbuffers/libflatbuffers.so.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/flatbuffers/libflatbuffers.so.2.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../base/widechar_width/libwidechar_width.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libcpuid-cmake/libcpuid.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/brotli-cmake/libbrotli.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/aws-s3-cmake/libaws_s3_checksums.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/poco-cmake/Foundation/lib_poco_foundation_pcre.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/unixodbc-cmake/libltdl.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/yaml-cpp-cmake/libyaml-cpp.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/arrow-cmake/libthrift.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_regex.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/boost-cmake/lib_boost_iostreams.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libxml2-cmake/liblibxml2.so
        DESTINATION lib)



set(CPACK_PACKAGE_VERSION 0.1.0)
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "local_engine_jni")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_CMAKE_GENERATOR Ninja)
set(CPACK_PACKAGE_CONTACT "neng.liu@kyligence.io")
set(CPACK_PACKAGE_VENDOR "Kyligence")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_RPM_PACKAGE_AUTOREQPROV "no")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
include(CPack)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
