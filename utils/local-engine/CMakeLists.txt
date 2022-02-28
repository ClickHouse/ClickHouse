set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
set(CLICKHOUSE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/src")
set(USE_INTERNAL_PARQUET_LIBRARY 1)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

cmake_minimum_required(VERSION 3.11)
if(CMAKE_VERSION VERSION_LESS 3.11)
    message(FATAL_ERROR "Building local engine JNI bindings requires CMake version >= 3.11")
endif()
include (${ClickHouse_SOURCE_DIR}/cmake/warnings.cmake)


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
        )
#include_directories("${ClickHouse_SOURCE_DIR}/programs")
target_include_directories(${LOCALENGINE_SHARED_LIB} PUBLIC ${ClickHouse_SOURCE_DIR}/src ${ClickHouse_SOURCE_DIR}/base)
target_link_libraries(${LOCALENGINE_SHARED_LIB} ${CLICKHOUSE_SERVER_LINK} loggers)

install( FILES
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/flatbuffers/libflatbuffers.so.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/flatbuffers/libflatbuffers.so.2.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/openldap-cmake/libldap_r.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/openldap-cmake/liblber.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/third_party/cares/cares/lib/libcares.so.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/grpc/third_party/cares/cares/lib/libcares.so.2.3.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/protobuf/libprotobuf.so.3.13.0.0
        ${CMAKE_CURRENT_BINARY_DIR}/../../libcassandra.so.2
        ${CMAKE_CURRENT_BINARY_DIR}/../../contrib/libuv/libuv.so.1
        ${CMAKE_CURRENT_BINARY_DIR}/../../utils/local-engine/cmake/libcxx/libcxx.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../utils/local-engine/cmake/libcxxabi/libcxxabi.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../utils/local-engine/cmake/libunwind/libunwind.so
        ${CMAKE_CURRENT_BINARY_DIR}/../../utils/local-engine/liblocal_engine_jni.so
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
