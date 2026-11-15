if (NOT OS_LINUX OR NOT ARCH_AMD64)
    message (STATUS "Not building memcpy-bench because it's not Linux on x86")
    return ()
endif ()

enable_language(ASM)

clickhouse_add_executable (memcpy-bench
    memcpy-bench.cpp
    FastMemcpy.cpp
    FastMemcpy_Avx.cpp
    glibc/memcpy-ssse3.S
    glibc/memcpy-ssse3-back.S
    glibc/memmove-sse2-unaligned-erms.S
    glibc/memmove-avx-unaligned-erms.S
    glibc/memmove-avx512-unaligned-erms.S
    glibc/memmove-avx512-no-vzeroupper.S
    )

add_compile_options(memcpy-bench PRIVATE -fno-tree-loop-distribute-patterns)

if (OS_SUNOS)
    target_compile_options(memcpy-bench PRIVATE "-Wa,--divide")
endif()

set_source_files_properties(FastMemcpy.cpp PROPERTIES COMPILE_FLAGS "-Wno-old-style-cast")
set_source_files_properties(FastMemcpy_Avx.cpp PROPERTIES COMPILE_FLAGS "-mavx -Wno-old-style-cast -Wno-cast-qual -Wno-cast-align")

# Suppress "marked as unused but used" warnings in memcpy-bench.cpp due to StringZilla dynamic dispatch setup
set_source_files_properties(memcpy-bench.cpp PROPERTIES COMPILE_FLAGS "-Wno-used-but-marked-unused")

target_include_directories(memcpy-bench PRIVATE .)
target_link_libraries(memcpy-bench PRIVATE clickhouse_common_io boost::program_options ch_contrib::stringzilla)

