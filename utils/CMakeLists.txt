if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

# Utils used in package
add_subdirectory (config-processor)
add_subdirectory (report)
add_subdirectory (syntax-analyzer)

# Not used in package
if (NOT DEFINED ENABLE_UTILS OR ENABLE_UTILS)
    add_subdirectory (compressor)
    add_subdirectory (iotest)
    add_subdirectory (corrector_utf8)
    add_subdirectory (zookeeper-cli)
    add_subdirectory (zookeeper-dump-tree)
    add_subdirectory (zookeeper-remove-by-list)
    add_subdirectory (zookeeper-create-entry-to-download-part)
    add_subdirectory (zookeeper-adjust-block-numbers-to-parts)
    add_subdirectory (wikistat-loader)
    add_subdirectory (check-marks)
    add_subdirectory (test-data-generator)
    add_subdirectory (convert-month-partitioned-parts)
    add_subdirectory (checksum-for-compressed-block)
    add_subdirectory (db-generator)
    add_subdirectory (wal-dump)
    add_subdirectory (check-mysql-binlog)
endif ()

if (ENABLE_CODE_QUALITY)
    add_subdirectory (check-style)
endif ()

add_subdirectory (package)
