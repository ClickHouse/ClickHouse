# Disable clang-tidy for protobuf generated files
set (CMAKE_CXX_CLANG_TIDY "")

if (USE_PROTOBUF)
    protobuf_generate_cpp(ProtobufDelimitedMessagesSerializer_Srcs ProtobufDelimitedMessagesSerializer_Hdrs ${CMAKE_CURRENT_SOURCE_DIR}/../../tests/queries/0_stateless/00825_protobuf_format.proto)
    protobuf_generate_cpp(ProtobufDelimitedMessagesSerializer_Srcs2 ProtobufDelimitedMessagesSerializer_Hdrs2 ${CMAKE_CURRENT_SOURCE_DIR}/../../tests/queries/0_stateless/00825_protobuf_format_syntax2.proto)
    add_executable (ProtobufDelimitedMessagesSerializer ProtobufDelimitedMessagesSerializer.cpp ${ProtobufDelimitedMessagesSerializer_Srcs} ${ProtobufDelimitedMessagesSerializer_Hdrs} ${ProtobufDelimitedMessagesSerializer_Srcs2} ${ProtobufDelimitedMessagesSerializer_Hdrs2})
    target_include_directories (ProtobufDelimitedMessagesSerializer SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
    target_link_libraries (ProtobufDelimitedMessagesSerializer PRIVATE ${Protobuf_LIBRARY} boost::program_options)
    get_filename_component(ProtobufDelimitedMessagesSerializer_OutputDir "${CMAKE_CURRENT_LIST_DIR}/../../tests/queries/0_stateless" REALPATH)
    target_compile_definitions(ProtobufDelimitedMessagesSerializer PRIVATE OUTPUT_DIR="${ProtobufDelimitedMessagesSerializer_OutputDir}")

    # Protoc generates substandard code.
    check_cxx_compiler_flag("-Wsuggest-destructor-override" HAS_SUGGEST_DESTRUCTOR_OVERRIDE)
    if (HAS_SUGGEST_OVERRIDE)
        target_compile_options(ProtobufDelimitedMessagesSerializer PRIVATE -Wno-suggest-destructor-override)
    endif()
endif ()
