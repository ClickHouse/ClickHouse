if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if (ENABLE_CLICKHOUSE_SELF_EXTRACTING)
    add_subdirectory (self-extracting-executable)
endif ()

if (USE_BINARY_HASH)
    add_subdirectory (hash-binary)
endif ()

if (ENABLE_EXTERN_LOCAL_ENGINE)
    add_subdirectory (extern-local-engine)
endif ()
    
# Not used in packages
if (ENABLE_UTILS)
    add_subdirectory (corrector_utf8)
    add_subdirectory (memcpy-bench)
endif ()
