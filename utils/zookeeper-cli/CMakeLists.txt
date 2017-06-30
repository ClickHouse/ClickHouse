add_executable(clickhouse-zookeeper-cli zookeeper-cli.cpp)
target_link_libraries(clickhouse-zookeeper-cli dbms ${LINE_EDITING_LIBS})
INSTALL(TARGETS clickhouse-zookeeper-cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse-utils)
