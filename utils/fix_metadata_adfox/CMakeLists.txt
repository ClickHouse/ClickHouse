add_executable (fix-metadata main.cpp)
target_compile_options(fix-metadata PRIVATE -Wno-format)
target_link_libraries (fix-metadata PRIVATE dbms clickhouse_common_zookeeper ${Boost_PROGRAM_OPTIONS_LIBRARY})
