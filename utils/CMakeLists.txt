if(MAKE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

# Utils used in package
add_subdirectory (config-processor)
add_subdirectory (report)


macro(add_object_library name common_path)
    if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
        add_headers_and_sources(dbms ${common_path})
    else ()
        list (APPEND all_modules ${name})
        add_headers_and_sources(${name} ${common_path})
        add_library(${name} SHARED ${${name}_sources} ${${name}_headers})
        target_link_libraries (${name} PRIVATE -Wl,--unresolved-symbols=ignore-all)
    endif ()
endmacro()


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
    add_subdirectory (fill-factor)
    add_subdirectory (check-marks)
    add_subdirectory (test-data-generator)
    add_subdirectory (convert-month-partitioned-parts)
    add_subdirectory (checksum-for-compressed-block)
    add_subdirectory (uatraits-fast)
endif ()

if (ENABLE_CODE_QUALITY)
    add_subdirectory (check-style)
endif ()

add_object_library(clickhouse_uatraitsfast uatraits-fast)

add_subdirectory (package)
