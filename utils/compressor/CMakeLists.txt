add_executable (compressor main.cpp)
target_link_libraries (compressor dbms ${BOOST_PROGRAM_OPTIONS_LIB})

INSTALL(TARGETS compressor RUNTIME DESTINATION bin COMPONENT compressor)


add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test zstd)
