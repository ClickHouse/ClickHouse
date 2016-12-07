add_executable (wikistat-loader main.cpp ${SRCS})
target_link_libraries (wikistat-loader dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
