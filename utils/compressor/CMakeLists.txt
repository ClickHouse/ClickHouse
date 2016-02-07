add_executable (compressor main.cpp ${SRCS})
target_link_libraries (compressor dbms libboost_program_options.a)

INSTALL(TARGETS compressor RUNTIME DESTINATION bin COMPONENT compressor)


