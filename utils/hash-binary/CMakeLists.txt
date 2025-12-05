# This tool reuses ClickHouse's SipHash.h header (and its dependencies).
# It must be compilable with the host compiler during cross-compilation.
add_executable(hash-binary hash-binary.cpp)

target_include_directories(hash-binary PRIVATE
    ${ClickHouse_SOURCE_DIR}/src
    ${ClickHouse_SOURCE_DIR}/base
    ${ClickHouse_SOURCE_DIR}/contrib/cityhash102/include
)

# Mark for native build when cross-compiling
add_native_target(hash-binary)
if (CMAKE_CROSSCOMPILING)
    set_target_properties(hash-binary PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/native/utils/hash-binary/hash-binary")
endif()
