add_test(NAME check-style COMMAND bash -c "${CMAKE_CURRENT_SOURCE_DIR}/check-style")
add_test(NAME check-include COMMAND bash -c "env RUN_DIR=${CMAKE_CURRENT_SOURCE_DIR} ROOT_DIR=${ClickHouse_SOURCE_DIR} BUILD_DIR=${ClickHouse_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/check-include")
