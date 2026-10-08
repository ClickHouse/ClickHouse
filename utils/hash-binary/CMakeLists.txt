# This is a standalone host tool with no ClickHouse dependencies.
# It must be compilable with the host compiler during cross-compilation.

if (CMAKE_CROSSCOMPILING)
    # When cross-compiling, we don't build hash-binary for the target.
    # It's built for the host in the native/ subdirectory during cmake configure.
    # We just need to register it so add_native_target knows about it.
    # Use EXCLUDE_FROM_ALL so it doesn't get built for the target architecture.
    add_executable(hash-binary EXCLUDE_FROM_ALL hash-binary.cpp)
else()
    add_executable(hash-binary hash-binary.cpp)
endif()

# Mark for native build when cross-compiling
add_native_target(hash-binary)
