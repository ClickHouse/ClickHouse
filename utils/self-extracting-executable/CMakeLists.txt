add_executable (pre_compressor compressor.cpp)
target_include_directories(pre_compressor PRIVATE .)
target_link_libraries(pre_compressor PUBLIC ch_contrib::zstd)

add_native_target (pre_compressor)

if (USE_MUSL)
    add_executable (decompressor decompressor.cpp $<TARGET_OBJECTS:musl-crt1> $<TARGET_OBJECTS:musl-crti> $<TARGET_OBJECTS:musl-crtn>)
else ()
    add_executable (decompressor decompressor.cpp)
endif ()
target_include_directories(decompressor PRIVATE .)
target_link_libraries(decompressor PUBLIC ch_contrib::zstd)

add_custom_target (compressor)
add_dependencies (compressor pre_compressor decompressor)

add_custom_command (TARGET compressor
    POST_BUILD
    COMMAND cat pre_compressor decompressor > compressor
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/post_build.sh >> compressor
    COMMAND chmod +x compressor
)
