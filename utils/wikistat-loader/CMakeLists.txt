add_executable (wikistat-loader main.cpp ${SRCS})
target_link_libraries (wikistat-loader dbms ${BOOST_PROGRAM_OPTIONS_LIB})
