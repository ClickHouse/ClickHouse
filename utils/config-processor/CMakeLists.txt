add_executable (config-processor config-processor.cpp)
target_link_libraries (config-processor dbms zkutil dbms)

INSTALL(TARGETS config-processor RUNTIME DESTINATION bin COMPONENT config-processor)
