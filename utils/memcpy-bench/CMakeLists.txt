enable_language(ASM)
add_executable (memcpy-bench memcpy-bench.cpp memcpy_jart.S)
#target_compile_options(memcpy-bench PRIVATE -mavx)
target_link_libraries(memcpy-bench PRIVATE dbms)

