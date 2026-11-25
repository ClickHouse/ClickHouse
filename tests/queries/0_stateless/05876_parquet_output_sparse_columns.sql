-- Tags: no-fasttest
-- no-fasttest: Parquet is not built in the fast test

-- The Parquet writer accepts sparse columns and converts them in its encoding threads.
-- The exports run over HTTP, where the server itself runs the Parquet writer.

DROP TABLE IF EXISTS t_mixed;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_full;

-- Part 1 is stored full, part 2 sparse, so a row group mixes both representations of a column.
CREATE TABLE t_mixed (id UInt64, u UInt64, s String, n Nullable(UInt32), tup Tuple(a UInt64, b String), lc LowCardinality(String), arr Array(UInt8))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, nullable_serialization_version = 'allow_sparse', serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192;
SYSTEM STOP MERGES t_mixed;
INSERT INTO t_mixed SELECT number, number + 1, toString(number + 1), number + 1, (number + 1, toString(number + 1)), toString(number % 3), [1] FROM numbers(100);
INSERT INTO t_mixed SELECT 100 + number, if(x, number + 1, 0), if(x, toString(number + 1), ''), if(x, number + 1, NULL),
    (if(x, number + 1, 0), if(x, toString(number + 1), '')), toString(number % 3), [] FROM (SELECT number, cityHash64(number) % 50 = 0 AS x FROM numbers(5000));

CREATE TABLE t_sparse (id UInt64, u UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_full (id UInt64, u UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sparse SELECT number, if(x, number, 0), if(x, toString(number), '') FROM (SELECT number, cityHash64(number) % 50 = 0 AS x FROM numbers(20000));
INSERT INTO t_full SELECT * FROM t_sparse;

SELECT 'kinds',
    (SELECT countIf(column = 'u' AND serialization_kind = 'Default') > 0 AND countIf(column = 'u' AND serialization_kind = 'Sparse') > 0
        AND countIf(column = 'n' AND serialization_kind = 'Sparse') > 0 AND countIf(column = 'tup' AND has(subcolumns.serializations, 'Sparse')) > 0
     FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_mixed' AND active)
    AND (SELECT countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse' AND active)
    AND (SELECT countIf(serialization_kind = 'Sparse') = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_full' AND active);

-- The writer gets the sparse columns as they are, unless the user setting asks the pipeline to convert them.
SELECT count() FROM url('http://localhost:' || toString(getServerPort('http_port')) || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_sparse+FORMAT+Parquet'
    || '&allow_special_serialization_kinds_in_output_formats=1&database=' || currentDatabase() || '&log_comment=' || currentDatabase() || '_sparse', Parquet, 'id UInt64, u UInt64, s String') FORMAT Null;
SELECT count() FROM url('http://localhost:' || toString(getServerPort('http_port')) || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_sparse+FORMAT+Parquet'
    || '&allow_special_serialization_kinds_in_output_formats=0&database=' || currentDatabase() || '&log_comment=' || currentDatabase() || '_sparse_off', Parquet, 'id UInt64, u UInt64, s String') FORMAT Null;
SELECT count() FROM url('http://localhost:' || toString(getServerPort('http_port')) || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_full+FORMAT+Parquet'
    || '&allow_special_serialization_kinds_in_output_formats=1&database=' || currentDatabase() || '&log_comment=' || currentDatabase() || '_full', Parquet, 'id UInt64, u UInt64, s String') FORMAT Null;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

WITH (SELECT sum(input_bytes) FROM system.processors_profile_log WHERE event_date >= yesterday() AND name = 'ParquetBlockOutputFormat' AND query_id IN
        (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = currentDatabase() || '_sparse')) AS sparse_bytes,
    (SELECT sum(input_bytes) FROM system.processors_profile_log WHERE event_date >= yesterday() AND name = 'ParquetBlockOutputFormat' AND query_id IN
        (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = currentDatabase() || '_sparse_off')) AS sparse_off_bytes,
    (SELECT sum(input_bytes) FROM system.processors_profile_log WHERE event_date >= yesterday() AND name = 'ParquetBlockOutputFormat' AND query_id IN
        (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = currentDatabase() || '_full')) AS full_bytes
SELECT 'sparse kept', sparse_bytes * 2 < full_bytes, 'sparse converted by the setting', sparse_off_bytes * 2 >= full_bytes;

-- The exported rows are the table's ones on every write path of the writer.
-- One reading stream keeps the order of the pieces: the full part first, then the sparse one.
SELECT 'parallel', (SELECT (count(), sum(cityHash64(*))) FROM t_mixed) = (SELECT (count(), sum(cityHash64(*))) FROM url('http://localhost:' || toString(getServerPort('http_port'))
    || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_mixed+FORMAT+Parquet&max_streams_for_merge_tree_reading=1&max_threads=4',
    Parquet, 'id UInt64, u UInt64, s String, n Nullable(UInt32), tup Tuple(a UInt64, b String), lc LowCardinality(String), arr Array(UInt8)'));
SELECT 'single thread', (SELECT (count(), sum(cityHash64(*))) FROM t_mixed) = (SELECT (count(), sum(cityHash64(*))) FROM url('http://localhost:' || toString(getServerPort('http_port'))
    || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_mixed+FORMAT+Parquet&max_streams_for_merge_tree_reading=1&max_threads=4&output_format_parquet_parallel_encoding=0',
    Parquet, 'id UInt64, u UInt64, s String, n Nullable(UInt32), tup Tuple(a UInt64, b String), lc LowCardinality(String), arr Array(UInt8)'));
SELECT 'max_threads 1', (SELECT (count(), sum(cityHash64(*))) FROM t_mixed) = (SELECT (count(), sum(cityHash64(*))) FROM url('http://localhost:' || toString(getServerPort('http_port'))
    || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_mixed+FORMAT+Parquet&max_streams_for_merge_tree_reading=1&max_threads=1',
    Parquet, 'id UInt64, u UInt64, s String, n Nullable(UInt32), tup Tuple(a UInt64, b String), lc LowCardinality(String), arr Array(UInt8)'));
SELECT 'split', (SELECT (count(), sum(cityHash64(*))) FROM t_mixed) = (SELECT (count(), sum(cityHash64(*))) FROM url('http://localhost:' || toString(getServerPort('http_port'))
    || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_mixed+FORMAT+Parquet&max_streams_for_merge_tree_reading=1&max_threads=4&output_format_parquet_row_group_size=1000',
    Parquet, 'id UInt64, u UInt64, s String, n Nullable(UInt32), tup Tuple(a UInt64, b String), lc LowCardinality(String), arr Array(UInt8)'));
SELECT 'row group bytes', (SELECT (count(), sum(cityHash64(*))) FROM t_mixed) = (SELECT (count(), sum(cityHash64(*))) FROM url('http://localhost:' || toString(getServerPort('http_port'))
    || '/?query=SELECT+*+FROM+' || currentDatabase() || '.t_mixed+FORMAT+Parquet&max_streams_for_merge_tree_reading=1&max_threads=4&output_format_parquet_row_group_size_bytes=100000',
    Parquet, 'id UInt64, u UInt64, s String, n Nullable(UInt32), tup Tuple(a UInt64, b String), lc LowCardinality(String), arr Array(UInt8)'));
SELECT 'array join', (SELECT (count(), sum(cityHash64(*))) FROM (SELECT id, s, a FROM t_mixed ARRAY JOIN range(3) AS a))
    = (SELECT (count(), sum(cityHash64(*))) FROM url('http://localhost:' || toString(getServerPort('http_port'))
    || '/?query=SELECT+id,+s,+a+FROM+' || currentDatabase() || '.t_mixed+ARRAY+JOIN+range(3)+AS+a+FORMAT+Parquet&max_streams_for_merge_tree_reading=1&max_threads=4&enable_lazy_columns_replication=1',
    Parquet, 'id UInt64, s String, a UInt8'));

DROP TABLE t_mixed;
DROP TABLE t_sparse;
DROP TABLE t_full;
