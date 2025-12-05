# This tool reuses ClickHouse's SipHash.h header (and its dependencies).
# It must be compilable with the host compiler during cross-compilation.

if (NOT CMAKE_CROSSCOMPILING)
    # When not cross-compiling, build hash-binary normally
    add_executable(hash-binary hash-binary.cpp)

    target_include_directories(hash-binary PRIVATE
        ${ClickHouse_SOURCE_DIR}/src
        ${ClickHouse_SOURCE_DIR}/base
        ${ClickHouse_SOURCE_DIR}/contrib/cityhash102/include
    )

    # Mark for native build (this registers the target to be built in the native build directory during cross-compilation)
    add_native_target(hash-binary)
else()
    # When cross-compiling, create an imported target pointing to the native-built binary
    add_executable(hash-binary IMPORTED GLOBAL)
    set_target_properties(hash-binary PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/native/utils/hash-binary/hash-binary")
endif()
