# memcpy_jart.S contains position dependent code
if (ENABLE_POSITION_INDEPENDENT_BINARY OR NOT OS_LINUX OR NOT ARCH_AMD64)
    message (STATUS "Not building memcpy-bench due to PIC enabled or not Linux or not x86")
    return ()
endif ()

enable_language(ASM)

clickhouse_add_executable (memcpy-bench
    memcpy-bench.cpp
    FastMemcpy.cpp
    FastMemcpy_Avx.cpp
    memcpy_jart.S
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

target_include_directories(memcpy-bench PRIVATE .)
target_link_libraries(memcpy-bench PRIVATE dbms clickhouse_functions boost::program_options)

