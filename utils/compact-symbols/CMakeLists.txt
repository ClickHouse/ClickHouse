# This is a standalone host tool with no ClickHouse library dependencies.

set(COMPACT_SYMBOLS_SOURCES
    compact-symbols.cpp
    ${ClickHouse_SOURCE_DIR}/src/Common/CompactSymbols.cpp)

if (NOT(
    CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
    AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR
    AND NOT USE_MUSL
))
    add_executable(compact-symbols EXCLUDE_FROM_ALL ${COMPACT_SYMBOLS_SOURCES})
else()
    add_executable(compact-symbols ${COMPACT_SYMBOLS_SOURCES})
endif()

target_include_directories(compact-symbols PRIVATE ${ClickHouse_SOURCE_DIR}/src)
target_link_libraries(compact-symbols PRIVATE ch_contrib::zstd)

if (SYMBOL_SECTIONS STREQUAL "compact")
    add_native_target(compact-symbols)
endif()
