if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if(USE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

if (ENABLE_CLICKHOUSE_SELF_EXTRACTING)
    add_subdirectory (self-extracting-executable)
endif ()

# Utils used in package
add_subdirectory (config-processor)
add_subdirectory (report)

message (WARNING "DEBUG 1 ${ENABLE_UTILS}")

# Not used in package
if (NOT DEFINED ENABLE_UTILS OR ENABLE_UTILS)
    add_subdirectory (compressor)
    add_subdirectory (iotest)
    add_subdirectory (corrector_utf8)
    add_subdirectory (zookeeper-cli)
    add_subdirectory (zookeeper-test)
    add_subdirectory (zookeeper-dump-tree)
    add_subdirectory (zookeeper-remove-by-list)
    add_subdirectory (zookeeper-create-entry-to-download-part)
    add_subdirectory (zookeeper-adjust-block-numbers-to-parts)
    add_subdirectory (wikistat-loader)
    add_subdirectory (check-marks)
    add_subdirectory (checksum-for-compressed-block)
    add_subdirectory (db-generator)
    add_subdirectory (wal-dump)
    add_subdirectory (check-mysql-binlog)
    add_subdirectory (keeper-bench)
    add_subdirectory (graphite-rollup)

    if (TARGET ch_contrib::nuraft)
        add_subdirectory (keeper-data-dumper)
    endif ()

    # memcpy_jart.S contains position dependent code
    if (NOT CMAKE_POSITION_INDEPENDENT_CODE AND OS_LINUX AND ARCH_AMD64)
        add_subdirectory (memcpy-bench)
    endif ()
endif ()

add_subdirectory (package)
