add_executable (compressor compressor.cpp)
target_link_libraries(compressor PUBLIC ch_contrib::zstd)

add_executable (decompressor decompressor.cpp)
target_link_libraries(decompressor PUBLIC ch_contrib::zstd)
