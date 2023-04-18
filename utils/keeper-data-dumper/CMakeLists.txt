set(KEEPER_DATA_DUMPER_EXTERNAL_SOURCES
    ${CMAKE_CURRENT_BINARY_DIR}/../../src/Storages/System/StorageSystemTimeZones.generated.cpp
)

clickhouse_add_executable(keeper-data-dumper
                          ${KEEPER_DATA_DUMPER_EXTERNAL_SOURCES}
                          main.cpp)
target_link_libraries(keeper-data-dumper PRIVATE dbms)
