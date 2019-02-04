add_executable (test-data-generator main.cpp)
target_link_libraries(test-data-generator PRIVATE clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_executable (markov-model markov-model.cpp)
target_link_libraries(markov-model PRIVATE clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if(USE_PROTOBUF)
    protobuf_generate_cpp(ProtobufDelimitedMessagesSerializer_Srcs ProtobufDelimitedMessagesSerializer_Hdrs ${CMAKE_CURRENT_SOURCE_DIR}/../../dbms/tests/queries/0_stateless/00825_protobuf_format.proto)
    protobuf_generate_cpp(ProtobufDelimitedMessagesSerializer_Srcs2 ProtobufDelimitedMessagesSerializer_Hdrs2 ${CMAKE_CURRENT_SOURCE_DIR}/../../dbms/tests/queries/0_stateless/00825_protobuf_format_syntax2.proto)
    add_executable (ProtobufDelimitedMessagesSerializer ProtobufDelimitedMessagesSerializer.cpp ${ProtobufDelimitedMessagesSerializer_Srcs} ${ProtobufDelimitedMessagesSerializer_Hdrs} ${ProtobufDelimitedMessagesSerializer_Srcs2} ${ProtobufDelimitedMessagesSerializer_Hdrs2})
    target_include_directories (ProtobufDelimitedMessagesSerializer SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
    target_link_libraries (ProtobufDelimitedMessagesSerializer PRIVATE ${Protobuf_LIBRARY})
endif()
