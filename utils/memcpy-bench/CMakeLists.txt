enable_language(ASM)

add_executable (memcpy-bench
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

target_link_libraries(memcpy-bench PRIVATE dbms boost::program_options)

