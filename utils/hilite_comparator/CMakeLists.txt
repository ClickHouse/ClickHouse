clickhouse_add_executable (
    hilite_comparator
    HiliteComparator.h
    HiliteComparator.cpp
)

target_link_libraries(hilite-comparator PRIVATE dbms)
