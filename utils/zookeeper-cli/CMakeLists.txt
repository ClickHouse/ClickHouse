add_executable(clickhouse-zookeeper-cli zookeeper-cli.cpp)
target_link_libraries(clickhouse-zookeeper-cli PRIVATE clickhouse_common_zookeeper)
INSTALL(TARGETS clickhouse-zookeeper-cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse-utils)
