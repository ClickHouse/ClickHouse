
set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")


macro(add_headers_and_sources_including_cc prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.cc ${common_path}/*.h)
endmacro()

add_headers_and_sources(substait_source .)
add_headers_and_sources_including_cc(ch_parquet arrow)
add_library(substait_source ${substait_source_sources})
target_compile_options(substait_source PUBLIC -fPIC
    -Wno-shadow-field-in-constructor
    -Wno-return-type
    -Wno-reserved-identifier
    -Wno-extra-semi-stmt
    -Wno-extra-semi
    -Wno-unused-result
    -Wno-unreachable-code-return
    -Wno-unused-parameter
    -Wno-unreachable-code
    -Wno-pessimizing-move
    -Wno-unreachable-code-break
    -Wno-unused-variable
    -Wno-inconsistent-missing-override
    -Wno-shadow-uncaptured-local
    -Wno-suggest-override
    -Wno-unused-member-function
    -Wno-deprecated-this-capture
)

target_link_libraries(substait_source PUBLIC
    boost::headers_only
    ch_contrib::protobuf
    clickhouse_common_io
    ch_contrib::hdfs
    substrait
)

target_include_directories(substait_source SYSTEM BEFORE PUBLIC 
    ${ARROW_INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/cpp/src
    ${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src
)