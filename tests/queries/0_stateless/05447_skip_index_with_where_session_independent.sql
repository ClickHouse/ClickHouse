-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- A partial index whose condition uses only comparisons on plain columns does not depend on session settings or time zones.

SET allow_experimental_partial_skip_indexes = 1;

DROP TABLE IF EXISTS t_partial_si;
CREATE TABLE t_partial_si (id UInt64, flag UInt8, INDEX i id TYPE minmax GRANULARITY 1 WHERE flag = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

-- The part is written by a session with another time zone.
INSERT INTO t_partial_si SELECT number, 1 FROM numbers(3) SETTINGS session_timezone = 'Asia/Tokyo';

SET use_query_condition_cache = 0, use_skip_indexes = 1, force_data_skipping_indices = 'i', max_rows_to_read = 1;
SELECT count() FROM t_partial_si WHERE flag = 1 AND id = 1 SETTINGS use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_partial_si WHERE flag = 1 AND id = 1 SETTINGS use_skip_indexes_on_data_read = 0, function_json_value_return_type_allow_complex = 1;
SELECT count() FROM t_partial_si WHERE flag = 1 AND id = 1 SETTINGS use_skip_indexes_on_data_read = 0, session_timezone = 'America/New_York';

DROP TABLE t_partial_si;
