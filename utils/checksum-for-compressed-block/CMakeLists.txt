clickhouse_add_executable (checksum-for-compressed-block-find-bit-flips main.cpp)
target_link_libraries(checksum-for-compressed-block-find-bit-flips PRIVATE dbms)
