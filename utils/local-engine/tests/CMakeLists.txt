set(USE_INTERNAL_GTEST_LIBRARY 0)
enable_testing()
include(CTest)
include (${PROJECT_SOURCE_DIR}/cmake/find/gtest.cmake)
message(GTEST_INCLUDE_DIRS:${GTEST_INCLUDE_DIRS})
include_directories(${GTEST_INCLUDE_DIRS})
macro (grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()
set(TEST_DATA_DIR "${ClickHouse_SOURCE_DIR}/utils/local-engine/tests")

configure_file(
        ${ClickHouse_SOURCE_DIR}/utils/local-engine/tests/testConfig.h.in
        ${ClickHouse_SOURCE_DIR}/utils/local-engine/tests/testConfig.h
)
# attach all dbms gtest sources
grep_gtest_sources("${ClickHouse_SOURCE_DIR}/utils/local_engine/tests" local_engine_gtest_sources)
add_executable(unit_tests_local_engine ${local_engine_gtest_sources})
target_compile_options(unit_tests_local_engine PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-covered-switch-default
        -Wno-undef
        -Wno-sign-compare
        -Wno-used-but-marked-unused
        -Wno-missing-noreturn
        -Wno-gnu-zero-variadic-macro-arguments
        )
target_include_directories(unit_tests_local_engine
        PRIVATE
        ${GTEST_INCLUDE_DIRS}/include
        ${builder_headers}
        ${parser_headers}
        )
target_link_libraries(unit_tests_local_engine ${CLICKHOUSE_SERVER_LINK} ${LOCALENGINE_SHARED_LIB} ${GTEST_BOTH_LIBRARIES})
add_check(unit_tests_local_engine)