set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
set(CLICKHOUSE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/src")
set(USE_INTERNAL_PARQUET_LIBRARY 1)
set(RAPIDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rapidjson/include")

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

get_filename_component(JAVA_MAIN_CLASS_PATH
        ${PROJECT_SOURCE_DIR}/utils/local-engine/java/src/main/java
        ABSOLUTE)
set(JNI_NATIVE_SOURCES
        local_engine_jni.cpp)

set(JAVA_MAIN_CLASSES
        ${JAVA_MAIN_CLASS_PATH}/io/kyligence/jni/engine/LocalEngine.java
        ${JAVA_MAIN_CLASS_PATH}/io/kyligence/jni/engine/SparkRowInfo.java
        )
# Create the jni header file (from the java class).
set(JNI_HEADERS_DIR ${PROJECT_SOURCE_DIR}/utils/local-engine/include)
file(MAKE_DIRECTORY ${JNI_HEADERS_DIR})
if(${Java_VERSION_MAJOR} VERSION_GREATER_EQUAL "10" AND ${CMAKE_VERSION} VERSION_LESS "3.11.4")
    # Java 10 and newer don't have javah, but the alternative GENERATE_NATIVE_HEADERS requires CMake 3.11.4 or newer
    message(FATAL_ERROR "Detected Java 10 or newer (${Java_VERSION_STRING}), to build with CMake please upgrade CMake to 3.11.4 or newer")

elseif(${CMAKE_VERSION} VERSION_LESS "3.11.4" OR (${Java_VERSION_MINOR} STREQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1"))
    # Old CMake or Java 1.7 prepare the JAR...
    message(FATAL_ERROR "Don't support old JDK ${Java_VERSION_STRING}")

else ()
    # Java 1.8 or newer prepare the JAR...
    message("Preparing Jar for JDK ${Java_VERSION_STRING}")
    add_jar(
            local_engine
            SOURCES ${JAVA_MAIN_CLASSES}
            GENERATE_NATIVE_HEADERS local_engine_headers DESTINATION ${JNI_HEADERS_DIR}
    )
    message("generating headers to ${JNI_HEADERS_DIR}")
endif()

set (CLICKHOUSE_SERVER_LINK
        PUBLIC
        #        dbms
        clickhouse_aggregate_functions
        clickhouse_common_io
        clickhouse_functions
        clickhouse_storages_system
        arrow_shared
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
#include (../../cmake/find/parquet.cmake)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/proto)
include_directories(${ClickHouse_SOURCE_DIR}/utils/local-engine)
#add_executable (local_engine
#        local_engine.cpp
#        ${builder_headers}
#        ${builder_sources}
#        ${parser_headers}
#        ${parser_sources}
#        )
#target_include_directories(local_engine PRIVATE
#        ${RAPIDJSON_INCLUDE_DIR}
#        ${SUBSTRAIT_HEADERS}
#        ${ARROW_INCLUDE_DIR}
#        )

include_directories(
        ${RAPIDJSON_INCLUDE_DIR}
        ${ARROW_INCLUDE_DIR}
        )



#target_link_libraries(local_engine ${CLICKHOUSE_SERVER_LINK} )
#create_javah()

set(LOCALENGINE_SHARED_LIB local_engine_jni)

add_library(${LOCALENGINE_SHARED_LIB} SHARED
        ${JNI_NATIVE_SOURCES}
        ${builder_headers}
        ${builder_sources}
        ${parser_headers}
        ${parser_sources}
        )
set_property(TARGET ${LOCALENGINE_SHARED_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
#add_executable(${LOCALENGINE_SHARED_LIB} ${JNI_NATIVE_SOURCES})
add_dependencies(${LOCALENGINE_SHARED_LIB} local_engine_headers)
target_link_libraries(${LOCALENGINE_SHARED_LIB} ${CLICKHOUSE_SERVER_LINK} )

install(TARGETS ${LOCALENGINE_SHARED_LIB}
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        PUBLIC_HEADER DESTINATION include
        )

install(TARGETS ${LOCALENGINE_SHARED_LIB} RUNTIME_DEPENDENCY_SET myset)
install(RUNTIME_DEPENDENCY_SET myset
        DIRECTORIES lib
        )

set(CPACK_PACKAGE_VERSION 0.1.0)
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "local_engine_jni")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "neng.liu@kyligence.io")
set(CPACK_PACKAGE_VENDOR "Kyligence")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
include(CPack)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
