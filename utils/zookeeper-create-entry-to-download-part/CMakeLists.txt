add_executable (zookeeper-create-entry-to-download-part main.cpp ${SRCS})
target_link_libraries (zookeeper-create-entry-to-download-part PRIVATE dbms clickhouse_common_zookeeper boost::program_options)
