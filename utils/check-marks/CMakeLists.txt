set(CHECK_MARKS_EXTERNAL_SOURCES
    ${CMAKE_CURRENT_BINARY_DIR}/../../src/Storages/System/StorageSystemTimeZones.generated.cpp
)

clickhouse_add_executable (check-marks ${CHECK_MARKS_EXTERNAL_SOURCES} main.cpp)
target_link_libraries(check-marks PRIVATE dbms boost::program_options)
