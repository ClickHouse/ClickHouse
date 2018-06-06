add_executable (config-processor config-processor.cpp)
target_link_libraries (config-processor clickhouse_common_config)

INSTALL(TARGETS config-processor RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT config-processor)
