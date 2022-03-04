macro (grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

set(USE_INTERNAL_GTEST_LIBRARY 0)
set(BENCHMARK_ENABLE_TESTING OFF)

enable_testing()
include(CTest)

include (${PROJECT_SOURCE_DIR}/cmake/find/gtest.cmake)
include_directories(${GTEST_INCLUDE_DIRS})

set(TEST_DATA_DIR "${ClickHouse_SOURCE_DIR}/utils/local-engine/tests")

configure_file(
        ${ClickHouse_SOURCE_DIR}/utils/local-engine/tests/testConfig.h.in
        ${ClickHouse_SOURCE_DIR}/utils/local-engine/tests/testConfig.h
)

include(FetchContent)
FetchContent_Declare(googlebenchmark GIT_REPOSITORY https://github.com/google/benchmark GIT_TAG master)
FetchContent_MakeAvailable(googlebenchmark)
include_directories(
        ${builder_headers}
        ${parser_headers}
)

grep_gtest_sources("${ClickHouse_SOURCE_DIR}/utils/local_engine/tests" local_engine_gtest_sources)

add_executable(unit_tests_local_engine ${local_engine_gtest_sources} )

add_executable(benchmark_local_engine benchmark_local_engine.cpp)

target_compile_options(unit_tests_local_engine PRIVATE
        -Wno-error
        )

target_compile_options(benchmark_local_engine PUBLIC
        -Wno-error
        )
target_compile_options(benchmark PUBLIC
        -Wno-error
        )


target_include_directories(unit_tests_local_engine PRIVATE
        ${GTEST_INCLUDE_DIRS}/include
        )
include_directories(benchmark_local_engine SYSTEM PUBLIC ${FETCH_CONTENT_SOURCE_DIR_GOOGLEBENCHMARK}/include)

target_link_libraries(unit_tests_local_engine PRIVATE ${LOCALENGINE_SHARED_LIB} ${GTEST_BOTH_LIBRARIES})
target_link_libraries(benchmark_local_engine PRIVATE ${LOCALENGINE_SHARED_LIB} benchmark::benchmark)
