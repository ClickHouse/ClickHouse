add_executable (compressor main.cpp)
target_link_libraries (compressor dbms libboost_program_options.a)

INSTALL(TARGETS compressor RUNTIME DESTINATION bin COMPONENT compressor)


add_executable (zstd_test zstd_test.cpp)
target_link_libraries (zstd_test zstd)
