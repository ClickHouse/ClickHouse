if (USE_MUSL)
    add_executable (pre_compressor ${MUSL_CRT_START_OBJECTS} compressor.cpp ${MUSL_CRT_END_OBJECTS})
else ()
    add_executable (pre_compressor compressor.cpp)
endif ()
target_include_directories(pre_compressor PRIVATE .)
target_link_libraries(pre_compressor PUBLIC ch_contrib::zstd)

add_native_target (pre_compressor)

if (USE_MUSL)
    add_executable (decompressor ${MUSL_CRT_START_OBJECTS} decompressor.cpp ${MUSL_CRT_END_OBJECTS})
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
