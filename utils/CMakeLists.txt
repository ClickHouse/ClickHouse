if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

set(MAX_LINKER_MEMORY 3500)
include(../cmake/limit_jobs.cmake)

if (ENABLE_CLICKHOUSE_SELF_EXTRACTING)
    add_subdirectory (self-extracting-executable)
endif ()

# Not used in packages
if (ENABLE_UTILS)
    add_subdirectory (corrector_utf8)
    add_subdirectory (memcpy-bench)
endif ()
