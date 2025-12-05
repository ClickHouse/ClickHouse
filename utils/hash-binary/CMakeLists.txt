# This is a standalone host tool with no ClickHouse dependencies.
# It must be compilable with the host compiler during cross-compilation.
add_executable(hash-binary hash-binary.cpp)

# Mark for native build when cross-compiling
add_native_target(hash-binary)
if (CMAKE_CROSSCOMPILING)
    set_target_properties(hash-binary PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/native/utils/hash-binary/hash-binary")
endif()
