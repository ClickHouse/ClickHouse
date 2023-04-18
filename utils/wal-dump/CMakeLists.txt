set(WAL_DUMP_EXTERNAL_SOURCES
    ${CMAKE_CURRENT_BINARY_DIR}/../../src/Storages/System/StorageSystemTimeZones.generated.cpp
)

clickhouse_add_executable (wal-dump ${WAL_DUMP_EXTERNAL_SOURCES} main.cpp)
target_link_libraries(wal-dump PRIVATE dbms boost::program_options)
