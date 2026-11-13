-- An INSERT computes the keys, skip indexes, TTL and `optimize_on_insert` merge of a MergeTree table
-- in the time zone of background merges, not in its `session_timezone`.
-- The INSERTs use two zones 17 hours apart, so at least one of them differs from the server time zone.
SET session_timezone = '';

DROP TABLE IF EXISTS t_key;
DROP TABLE IF EXISTS t_minute;
DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_agg_proj;
DROP TABLE IF EXISTS t_index;
DROP TABLE IF EXISTS t_index_rmt;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_ttl;
DROP TABLE IF EXISTS t_graphite_ref;
DROP TABLE IF EXISTS t_graphite_tokyo;
DROP TABLE IF EXISTS t_graphite_la;
DROP TABLE IF EXISTS t_created_in_session;
DROP TABLE IF EXISTS t_lambda;
DROP TABLE IF EXISTS t_set;
DROP TABLE IF EXISTS t_null_in;
DROP TABLE IF EXISTS t_dynamic;

SELECT 'sorting key over a time zone dependent conversion';
CREATE TABLE t_key (ts UInt32, id UInt32) ENGINE = MergeTree ORDER BY (toDate(toDateTime(ts)), id) SETTINGS index_granularity = 4;
INSERT INTO t_key SELECT 1704067200 + number * 3600, 1000 - number FROM numbers(72) SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_key SELECT 1704067200 + number * 3600, 1000 - number FROM numbers(72) SETTINGS session_timezone = 'America/Los_Angeles';
SELECT count() = (SELECT countIf(toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 86400))) FROM t_key) FROM t_key WHERE toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 86400));
SELECT count() = (SELECT countIf(toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 2 * 86400))) FROM t_key) FROM t_key WHERE toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 2 * 86400));
OPTIMIZE TABLE t_key FINAL;
SELECT count() = (SELECT countIf(toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 86400))) FROM t_key) FROM t_key WHERE toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 86400));
SELECT count() = (SELECT countIf(toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 2 * 86400))) FROM t_key) FROM t_key WHERE toDate(toDateTime(ts)) = toDate(toDateTime(1704067200 + 2 * 86400));

SELECT 'sorting key over a function reading the time zone when it runs';
CREATE TABLE t_minute (c0 Date) ENGINE = MergeTree ORDER BY toRelativeMinuteNum(c0) SETTINGS index_granularity = 64;
INSERT INTO t_minute SELECT CAST(number AS Date) FROM numbers(349) SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_minute SELECT CAST(number AS Date) FROM numbers(349) SETTINGS session_timezone = 'America/Los_Angeles';
SELECT count() FROM t_minute WHERE c0 = '1970-01-01';
OPTIMIZE TABLE t_minute FINAL;
SELECT count() FROM t_minute WHERE c0 = '1970-01-01';

SELECT 'projection sorting key';
CREATE TABLE t_proj (c0 Date, k UInt32, PROJECTION p (SELECT c0, k ORDER BY toRelativeMinuteNum(c0))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_proj SELECT CAST(number AS Date), number FROM numbers(349) SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_proj SELECT CAST(number AS Date), number FROM numbers(349) SETTINGS session_timezone = 'America/Los_Angeles';
SELECT count() FROM t_proj WHERE c0 = '1970-01-01' SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p';
OPTIMIZE TABLE t_proj FINAL;
SELECT count() FROM t_proj;

SELECT 'aggregate projection';
CREATE TABLE t_agg_proj (d Date, PROJECTION p (SELECT toRelativeMinuteNum(d) AS m, count() GROUP BY m)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_agg_proj SELECT CAST(number AS Date) FROM numbers(349) SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 4;
INSERT INTO t_agg_proj SELECT CAST(number AS Date) FROM numbers(349) SETTINGS session_timezone = 'America/Los_Angeles', max_threads = 4;
SELECT count(), sum(c) FROM (SELECT toRelativeMinuteNum(d) AS m, count() AS c FROM t_agg_proj GROUP BY m) SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p';

SELECT 'skip index, before and after the optimize_on_insert merge';
CREATE TABLE t_index (k UInt32, d Date, INDEX i toRelativeHourNum(d) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_index_rmt (k UInt32, d Date, INDEX i toRelativeHourNum(d) TYPE minmax GRANULARITY 1) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 1;
-- A merge would rebuild the indexes, and deduplicate the rows of the ReplacingMergeTree.
SYSTEM STOP MERGES t_index;
SYSTEM STOP MERGES t_index_rmt;
INSERT INTO t_index SELECT number, toDate('2024-01-01') + number FROM numbers(5) SETTINGS session_timezone = 'Asia/Tokyo', materialize_skip_indexes_on_insert = 1, optimize_on_insert = 1;
INSERT INTO t_index SELECT number, toDate('2024-01-01') + number FROM numbers(5) SETTINGS session_timezone = 'America/Los_Angeles', materialize_skip_indexes_on_insert = 1, optimize_on_insert = 1;
INSERT INTO t_index_rmt SELECT number, toDate('2024-01-01') + number FROM numbers(5) SETTINGS session_timezone = 'Asia/Tokyo', materialize_skip_indexes_on_insert = 1, optimize_on_insert = 1;
INSERT INTO t_index_rmt SELECT number, toDate('2024-01-01') + number FROM numbers(5) SETTINGS session_timezone = 'America/Los_Angeles', materialize_skip_indexes_on_insert = 1, optimize_on_insert = 1;
SELECT count() FROM t_index WHERE toRelativeHourNum(d) = toRelativeHourNum(toDate('2024-01-03')) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';
SELECT count() FROM t_index_rmt WHERE toRelativeHourNum(d) = toRelativeHourNum(toDate('2024-01-03')) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';

SELECT 'partition key';
CREATE TABLE t_part (d Date) ENGINE = MergeTree PARTITION BY toRelativeHourNum(d) ORDER BY tuple();
INSERT INTO t_part SELECT toDate('2024-01-02') SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_part SELECT toDate('2024-01-02') SETTINGS session_timezone = 'America/Los_Angeles';
INSERT INTO t_part SELECT toDate('2024-01-02');
SELECT uniqExact(partition_id) FROM system.parts WHERE database = currentDatabase() AND table = 't_part' AND active;
SELECT count() FROM t_part WHERE d = '2024-01-02';

SELECT 'TTL';
CREATE TABLE t_ttl (d Date) ENGINE = MergeTree ORDER BY tuple() TTL toDateTime(d) + INTERVAL 10 YEAR;
INSERT INTO t_ttl SELECT toDate('2024-01-02') SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_ttl SELECT toDate('2024-01-02') SETTINGS session_timezone = 'America/Los_Angeles';
SELECT groupUniqArray(delete_ttl_info_max = (SELECT max(toDateTime(d) + INTERVAL 10 YEAR) FROM t_ttl)) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl' AND active;

SELECT 'optimize_on_insert merge of GraphiteMergeTree';
-- `graphite_rollup` rounds rows older than 2 days to 6000 second windows that start at midnight of a UInt32 time.
CREATE TABLE t_graphite_ref (Path String, Time UInt32, Value Float64, Version UInt32) ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
CREATE TABLE t_graphite_tokyo (Path String, Time UInt32, Value Float64, Version UInt32) ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
CREATE TABLE t_graphite_la (Path String, Time UInt32, Value Float64, Version UInt32) ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
INSERT INTO t_graphite_ref SELECT 'sum_1', 1577836800 + number * 60, 1, 1 FROM numbers(1440) SETTINGS optimize_on_insert = 1;
INSERT INTO t_graphite_tokyo SELECT 'sum_1', 1577836800 + number * 60, 1, 1 FROM numbers(1440) SETTINGS optimize_on_insert = 1, session_timezone = 'Asia/Tokyo';
INSERT INTO t_graphite_la SELECT 'sum_1', 1577836800 + number * 60, 1, 1 FROM numbers(1440) SETTINGS optimize_on_insert = 1, session_timezone = 'America/Los_Angeles';
SELECT (SELECT groupArray((Time, v)) FROM (SELECT Time, sum(Value) AS v FROM t_graphite_tokyo GROUP BY Time ORDER BY Time))
    = (SELECT groupArray((Time, v)) FROM (SELECT Time, sum(Value) AS v FROM t_graphite_ref GROUP BY Time ORDER BY Time));
SELECT (SELECT groupArray((Time, v)) FROM (SELECT Time, sum(Value) AS v FROM t_graphite_la GROUP BY Time ORDER BY Time))
    = (SELECT groupArray((Time, v)) FROM (SELECT Time, sum(Value) AS v FROM t_graphite_ref GROUP BY Time ORDER BY Time));

SELECT 'sorting key over Dynamic DateTime values that the INSERT also serializes';
CREATE TABLE t_dynamic (id UInt32, d Array(Dynamic), s String) ENGINE = MergeTree ORDER BY toString(d) SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_dynamic;
-- One row per block, so the key and `s` serialize `d` in turn on one thread: here the key of rows 0-3 comes first.
INSERT INTO t_dynamic SELECT number, [CAST(toDateTime(1704067200 + number * 3600) AS Dynamic)] AS d, if(number < 4, '', toString(d)) FROM numbers(8)
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1, max_insert_threads = 1, max_block_size = 1, min_insert_block_size_rows = 0,
        min_insert_block_size_bytes = 0, optimize_trivial_insert_select = 0, short_circuit_function_evaluation = 'force_enable';
-- Here `s` of row 0 comes first.
INSERT INTO t_dynamic SELECT number + 8, [CAST(toDateTime(1704067200 + number * 3600) AS Dynamic)] AS d, toString(d) FROM numbers(8)
    SETTINGS session_timezone = 'America/Los_Angeles', max_threads = 1, max_insert_threads = 1, max_block_size = 1, min_insert_block_size_rows = 0,
        min_insert_block_size_bytes = 0, optimize_trivial_insert_select = 0;
SELECT s FROM t_dynamic WHERE id IN (4, 12) ORDER BY id;
SELECT id FROM t_dynamic WHERE toString(d) = toString([CAST(toDateTime(1704067200 + 4 * 3600) AS Dynamic)]) ORDER BY id SETTINGS force_primary_key = 1;

SELECT 'table created and filled in one session';
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE t_created_in_session (ts UInt32, id UInt32, INDEX i toDate(toDateTime(ts)) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY (toDate(toDateTime(ts)), id) SETTINGS index_granularity = 4;
INSERT INTO t_created_in_session SELECT 1704067200 + number * 3600, 1000 - number FROM numbers(72);
INSERT INTO t_created_in_session SELECT 1704067200 + number * 3600, 1000 - number FROM numbers(72);
OPTIMIZE TABLE t_created_in_session FINAL;
SET session_timezone = '';
SELECT count() FROM t_created_in_session;

SELECT 'skip index next to a sorting key, both with a lambda';
CREATE TABLE t_lambda (a Array(UInt32), INDEX i arraySum(arrayMap(x -> x * 2, a)) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY arraySum(arrayMap(x -> x + 1, a)) SETTINGS index_granularity = 1;
INSERT INTO t_lambda SELECT [number] FROM numbers(10);
SELECT count() FROM t_lambda WHERE arraySum(arrayMap(x -> x * 2, a)) = 10 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';

SELECT 'skip index next to a sorting key, both with a literal set';
CREATE TABLE t_set (k UInt32, INDEX i (k IN (3, 4)) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY ((k IN (1, 2)), k) SETTINGS index_granularity = 1;
INSERT INTO t_set SELECT number FROM numbers(10);
SELECT count() FROM t_set WHERE (k IN (3, 4)) = 1 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';

SELECT 'sorting key whose expression was rebuilt with transform_null_in';
SET transform_null_in = 1;
CREATE TABLE t_null_in (x UInt64) ENGINE = MergeTree ORDER BY (x IN (NULL, 1));
ALTER TABLE t_null_in COMMENT COLUMN x 'rebuilds the key expression under transform_null_in = 1';
SET transform_null_in = 0;
-- One INSERT: two parts would start a merge, and merges of this table fail.
INSERT INTO t_null_in VALUES (1), (2);
SELECT x FROM t_null_in ORDER BY x;

DROP TABLE t_key;
DROP TABLE t_minute;
DROP TABLE t_proj;
DROP TABLE t_agg_proj;
DROP TABLE t_index;
DROP TABLE t_index_rmt;
DROP TABLE t_part;
DROP TABLE t_ttl;
DROP TABLE t_graphite_ref;
DROP TABLE t_graphite_tokyo;
DROP TABLE t_graphite_la;
DROP TABLE t_created_in_session;
DROP TABLE t_lambda;
DROP TABLE t_set;
DROP TABLE t_null_in;
DROP TABLE t_dynamic;
