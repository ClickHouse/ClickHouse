set(protobuf_generate_PROTOC_OUT_DIR "${ClickHouse_SOURCE_DIR}/utils/local-engine/Substrait")
file(GLOB PROTOBUF_DEFINITION_FILES "/home/kyligence/Documents/code/substrait/binary/*.proto")
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
PROTOBUF_GENERATE_CPP(SUBSTRAIT_SRCS SUBSTRAIT_HEADERS ${PROTOBUF_DEFINITION_FILES})
add_library(substrait ${SUBSTRAIT_SRCS})
target_include_directories(substrait PRIVATE ${PROTOBUF_INCLUDE_DIR})
target_link_libraries(substrait libprotobuf)

