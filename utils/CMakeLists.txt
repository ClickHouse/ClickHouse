if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if(MAKE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

# Utils used in package
add_subdirectory (config-processor)
add_subdirectory (report)

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
    add_subdirectory (convert-month-partitioned-parts)
    add_subdirectory (checksum-for-compressed-block)
    add_subdirectory (db-generator)
    add_subdirectory (wal-dump)
    add_subdirectory (check-mysql-binlog)
    add_subdirectory (keeper-bench)

    if (USE_NURAFT)
        add_subdirectory (keeper-data-dumper)
    endif ()

    # memcpy_jart.S contains position dependent code
    if (NOT CMAKE_POSITION_INDEPENDENT_CODE AND NOT OS_DARWIN AND NOT OS_SUNOS AND NOT ARCH_AARCH64)
        add_subdirectory (memcpy-bench)
    endif ()
endif ()

add_subdirectory (package)
