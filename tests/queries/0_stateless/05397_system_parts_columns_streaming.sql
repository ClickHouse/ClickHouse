-- The readers of the `system.parts` family return the rows in chunks of at most `max_block_size` rows
-- and `preferred_block_size_bytes` bytes, and `system.parts_columns` builds only the selected columns.
-- The results are compared with the same rows read in a single chunk with all columns selected.

DROP TABLE IF EXISTS t_parts_stream;
DROP TABLE IF EXISTS t_parts_stream_full;
DROP TABLE IF EXISTS t_parts_columns_stream_full;
DROP TABLE IF EXISTS t_parts_stream_memory;

-- Arrays of values of different types become tuples of JSON with many substreams.
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;

CREATE TABLE t_parts_stream
(
    id UInt64 STATISTICS(basic),
    s String,
    t Tuple(a UInt32, b Array(String)),
    j JSON,
    PROJECTION p (SELECT id, s ORDER BY s)
)
ENGINE = MergeTree ORDER BY id PARTITION BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replace_long_file_name_to_hash = 1;

INSERT INTO t_parts_stream SELECT number, toString(number), (number, [toString(number)]), '{"a" : [{"x" : 1}, {"y" : 2}, {"z" : 3}, "str"], "b" : {"c" : [1, "str"]}}' FROM numbers(4)
SETTINGS materialize_statistics_on_insert = 1;

CREATE TABLE t_parts_stream_full ENGINE = Memory AS
SELECT name, partition, rows, bytes_on_disk, projections, _state AS state
FROM system.parts WHERE database = currentDatabase() AND table = 't_parts_stream';

CREATE TABLE t_parts_columns_stream_full ENGINE = Memory AS
SELECT
    name, column, type, column_position,
    column_bytes_on_disk, column_data_compressed_bytes, column_data_uncompressed_bytes, column_marks_bytes,
    statistics, serialization_kind, substreams, filenames,
    subcolumns.names AS subcolumn_names,
    subcolumns.types AS subcolumn_types,
    subcolumns.serializations AS subcolumn_serializations,
    subcolumns.bytes_on_disk AS subcolumn_bytes_on_disk,
    subcolumns.data_compressed_bytes AS subcolumn_data_compressed_bytes,
    subcolumns.data_uncompressed_bytes AS subcolumn_data_uncompressed_bytes,
    subcolumns.marks_bytes AS subcolumn_marks_bytes,
    _state AS state
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream';

SELECT 'fixture',
    count(),
    countIf(column = 'j' AND length(substreams) > 10),
    countIf(length(substreams) = length(filenames)),
    countIf(column = 't' AND has(subcolumn_names, 'b.size0') AND arraySum(subcolumn_bytes_on_disk) > 0),
    countIf(column = 'id' AND has(statistics, 'Basic'))
FROM t_parts_columns_stream_full;

-- Each part becomes a separate chunk.
SELECT 'parts chunks', uniqExact(blockNumber()), count()
FROM system.parts WHERE database = currentDatabase() AND table = 't_parts_stream'
SETTINGS max_block_size = 1;

SELECT 'parts_columns chunks by rows', uniqExact(blockNumber()), count()
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
SETTINGS max_block_size = 1;

SELECT 'parts_columns chunks by bytes', uniqExact(blockNumber()), count()
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
SETTINGS preferred_block_size_bytes = 1;

SELECT 'projection_parts chunks', uniqExact(blockNumber()), count()
FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_parts_stream'
SETTINGS max_block_size = 1;

SELECT 'projection_parts_columns chunks', uniqExact(blockNumber()), count()
FROM system.projection_parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
SETTINGS preferred_block_size_bytes = 1;

-- The rows read in many chunks are the same.
SELECT 'parts streamed', count() FROM
(
    SELECT name, partition, rows, bytes_on_disk, projections, _state
    FROM system.parts WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT
    SELECT * FROM t_parts_stream_full
)
SETTINGS max_block_size = 1;

SELECT 'parts_columns streamed', count() FROM
(
    SELECT
        name, column, type, column_position,
        column_bytes_on_disk, column_data_compressed_bytes, column_data_uncompressed_bytes, column_marks_bytes,
        statistics, serialization_kind, substreams, filenames,
        subcolumns.names, subcolumns.types, subcolumns.serializations,
        subcolumns.bytes_on_disk, subcolumns.data_compressed_bytes, subcolumns.data_uncompressed_bytes, subcolumns.marks_bytes,
        _state
    FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT
    SELECT * FROM t_parts_columns_stream_full
)
SETTINGS preferred_block_size_bytes = 1;

-- A column selected without the others of its group has the same values.
SELECT 'projections', count() FROM
(
    SELECT name, projections FROM system.parts WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, projections FROM t_parts_stream_full
);

SELECT 'column_bytes_on_disk', count() FROM
(
    SELECT name, column, column_bytes_on_disk FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, column_bytes_on_disk FROM t_parts_columns_stream_full
);

SELECT 'column_marks_bytes', count() FROM
(
    SELECT name, column, column_marks_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, column_marks_bytes FROM t_parts_columns_stream_full
);

SELECT 'statistics', count() FROM
(
    SELECT name, column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, statistics FROM t_parts_columns_stream_full
);

SELECT 'substreams', count() FROM
(
    SELECT name, column, substreams FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, substreams FROM t_parts_columns_stream_full
);

SELECT 'filenames', count() FROM
(
    SELECT name, column, filenames FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, filenames FROM t_parts_columns_stream_full
);

SELECT 'subcolumns.names', count() FROM
(
    SELECT name, column, subcolumns.names FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, subcolumn_names FROM t_parts_columns_stream_full
);

SELECT 'subcolumns.types', count() FROM
(
    SELECT name, column, subcolumns.types FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, subcolumn_types FROM t_parts_columns_stream_full
);

SELECT 'subcolumns.bytes_on_disk', count() FROM
(
    SELECT name, column, subcolumns.bytes_on_disk FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, subcolumn_bytes_on_disk FROM t_parts_columns_stream_full
);

SELECT 'subcolumns.marks_bytes', count() FROM
(
    SELECT name, column, subcolumns.marks_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, subcolumn_marks_bytes FROM t_parts_columns_stream_full
);

SELECT 'substreams with _state', count() FROM
(
    SELECT name, column, substreams, _state FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream'
    EXCEPT SELECT name, column, substreams, state FROM t_parts_columns_stream_full
);

-- The memory is bounded by the rows of about one part. Every substream name of a JSON path repeats the
-- long path name, so the table has about 60 MB of substream names, about 6 MB per part.
CREATE TABLE t_parts_stream_memory (id UInt64, j JSON)
ENGINE = MergeTree ORDER BY id PARTITION BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_parts_stream_memory
SELECT number, ('{' || arrayStringConcat(arrayMap(p -> '"' || repeat('k', 10000) || toString(p) || '" : 1', range(100)), ', ') || '}')::JSON
FROM numbers(10);

SELECT 'memory', column, avg(length(substreams)) > 100
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_parts_stream_memory'
GROUP BY column ORDER BY column
SETTINGS max_memory_usage = 50000000;

DROP TABLE t_parts_stream;
DROP TABLE t_parts_stream_full;
DROP TABLE t_parts_columns_stream_full;
DROP TABLE t_parts_stream_memory;
