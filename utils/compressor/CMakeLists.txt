
include_directories (BEFORE ${CMAKE_SOURCE_DIR}/contrib/libzstd/include)

add_executable (compressor main.cpp)
target_link_libraries (compressor dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})

install (TARGETS compressor RUNTIME DESTINATION bin COMPONENT compressor)

add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test zstd)
