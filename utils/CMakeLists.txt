if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

# Utils used in package
add_subdirectory (config-processor)
add_subdirectory (report)

# Not used in package
if (NOT DEFINED ENABLE_UTILS OR ENABLE_UTILS)
    add_subdirectory (compressor)
    add_subdirectory (iotest)
    add_subdirectory (corrector_utf8)
    add_subdirectory (zookeeper-cli)
    add_subdirectory (zookeeper-dump-tree)
    add_subdirectory (zookeeper-remove-by-list)
    add_subdirectory (zookeeper-create-entry-to-download-part)
    add_subdirectory (wikistat-loader)
    add_subdirectory (fill-factor)
    add_subdirectory (check-marks)
    add_subdirectory (test-data-generator)
endif ()

if (ENABLE_CODE_QUALITY)
    add_subdirectory (check-style)
endif ()
