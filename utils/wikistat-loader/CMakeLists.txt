add_executable (wikistat-loader main.cpp ${SRCS})
target_link_libraries (wikistat-loader PRIVATE clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
