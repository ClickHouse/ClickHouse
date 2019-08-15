add_executable (zookeeper-adjust-block-numbers-to-parts main.cpp ${SRCS})
target_link_libraries (zookeeper-adjust-block-numbers-to-parts PRIVATE dbms clickhouse_common_zookeeper ${Boost_PROGRAM_OPTIONS_LIBRARY})
