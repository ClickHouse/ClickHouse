clickhouse_add_executable (zookeeper-adjust-block-numbers-to-parts main.cpp ${SRCS})
target_compile_options(zookeeper-adjust-block-numbers-to-parts PRIVATE -Wno-format)
target_link_libraries (zookeeper-adjust-block-numbers-to-parts PRIVATE clickhouse_aggregate_functions dbms clickhouse_common_zookeeper boost::program_options)
