
set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")

macro(add_headers_and_sources_including_cc prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.cc ${common_path}/*.h)
endmacro()

add_headers_and_sources(ch_parquet .)
add_headers_and_sources_including_cc(ch_parquet arrow)

add_library(ch_parquet ${ch_parquet_sources})

target_compile_options(ch_parquet PUBLIC -fPIC
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

target_link_libraries(ch_parquet PUBLIC
    boost::headers_only
    clickhouse_common_io
)

target_include_directories(ch_parquet SYSTEM BEFORE PUBLIC 
    ${ARROW_INCLUDE_DIR} 
    ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/cpp/src
    ${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src
)