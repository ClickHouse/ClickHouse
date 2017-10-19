add_executable (load-catboost load-catboost.cpp ${SRCS})
target_link_libraries (load-catboost dbms)
