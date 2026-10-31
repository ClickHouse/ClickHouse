# This is a standalone host tool with no ClickHouse library dependencies.

set(COMPACT_SYMBOLS_SOURCES
    compact-symbols.cpp
    ${ClickHouse_SOURCE_DIR}/src/Common/CompactSymbols.cpp)

# Build the tool for the target only when it can run on the host (host == target). This must match
# the native-build guard in the root CMakeLists.txt: a native musl build (host == target) does not
# produce a native/ subtree, and its own statically linked compact-symbols runs on the host.
if (NOT(
    CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
    AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR
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
