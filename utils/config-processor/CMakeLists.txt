add_executable (config-processor config-processor.cpp)
target_link_libraries (config-processor dbms)

INSTALL(TARGETS config-processor RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT config-processor)
