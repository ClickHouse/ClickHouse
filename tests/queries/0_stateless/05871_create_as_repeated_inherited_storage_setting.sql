-- `CREATE TABLE ... AS` with its own settings inherits the last occurrence of a setting the source repeats, the one the source applies.

CREATE TABLE src (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 1, min_bytes_for_wide_part = 2;
CREATE TABLE dst AS src SETTINGS index_granularity = 128;

SELECT extractAll(create_table_query, '(?:min_bytes_for_wide_part|index_granularity) = \\d+') FROM system.tables WHERE database = currentDatabase() AND name = 'dst';
