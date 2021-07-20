add_executable(zk-test main.cpp)
target_link_libraries(zk-test PRIVATE clickhouse_common_zookeeper)
INSTALL(TARGETS zk-test RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse-utils)
