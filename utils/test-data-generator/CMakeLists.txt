add_executable (test-data-generator main.cpp)
target_link_libraries (test-data-generator dbms libboost_program_options.a)

add_executable (markov-model markov-model.cpp)
target_link_libraries (markov-model dbms libboost_program_options.a)
