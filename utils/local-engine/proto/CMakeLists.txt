file(GLOB protobuf_files
        substrait/*.proto
        substrait/extensions/*.proto
        )
FOREACH(FIL ${protobuf_files})
    file(RELATIVE_PATH FIL_RELATIVE ${ClickHouse_SOURCE_DIR}/utils/local-engine/proto/ ${FIL})
    string(REGEX REPLACE "\\.proto" "" FILE_NAME ${FIL_RELATIVE})
    LIST(APPEND SUBSTRAIT_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}.pb.cc")
    LIST(APPEND SUBSTRAIT_HEADERS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}.pb.h")
ENDFOREACH()

add_custom_target(
        generate_substrait
        COMMAND  protobuf::protoc -I${CMAKE_CURRENT_SOURCE_DIR} -I${ClickHouse_SOURCE_DIR}/contrib/protobuf/src --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/ ${protobuf_files}
        DEPENDS protobuf::protoc
        COMMENT "Running cpp protocol buffer compiler"
        VERBATIM )

include_directories(${Protobuf_INCLUDE_DIRS})

set_source_files_properties(${SUBSTRAIT_SRCS} PROPERTIES GENERATED TRUE)
add_library(substrait ${SUBSTRAIT_SRCS})
add_dependencies(substrait generate_substrait)
target_include_directories(substrait PRIVATE ${PROTOBUF_INCLUDE_DIR})
target_include_directories(substrait PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/)
target_link_libraries(substrait libprotobuf)

