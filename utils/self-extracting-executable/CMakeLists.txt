add_executable (pre_compressor compressor.cpp)
target_link_libraries(pre_compressor PUBLIC ch_contrib::zstd)

add_native_target (pre_compressor)

add_executable (decompressor decompressor.cpp)
target_link_libraries(decompressor PUBLIC ch_contrib::zstd)

add_custom_target (compressor)
add_dependencies (compressor pre_compressor decompressor)

add_custom_command (TARGET compressor
    POST_BUILD
    COMMAND cat pre_compressor decompressor > compressor
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/post_build.sh >> compressor
    COMMAND chmod +x compressor
)
