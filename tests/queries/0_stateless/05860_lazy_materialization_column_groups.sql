-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test counts the readers of the lazy read on the initiator, and with parallel replicas the
-- replicas share the rows, so that count depends on how the work is split.

-- Lazy materialization reads the columns of a few MergeTree rows in several column groups at once.
-- The result must not depend on query_plan_max_column_groups_for_lazy_materialization.

SET max_threads = 4, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;
-- The column groups are bounded by the threads and the streams of the main read: keep them independent of the free
-- memory of the server and of the query condition cache, which narrows the main read of a repeated query.
SET max_threads_min_free_memory_per_thread = 0, use_query_condition_cache = 0;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_lazy_column_groups;
DROP TABLE IF EXISTS t_lazy_column_groups_r;
DROP TABLE IF EXISTS t_lazy_column_groups_default;
DROP TABLE IF EXISTS t_lazy_column_groups_compact;
DROP TABLE IF EXISTS t_lazy_column_groups_delete;
DROP TABLE IF EXISTS t_lazy_column_groups_projection;
DROP TABLE IF EXISTS t_lazy_column_groups_pending;
DROP TABLE IF EXISTS t_lazy_column_groups_finished;

CREATE TABLE t_lazy_column_groups
(
    k UInt64,
    t DateTime,
    s1 String, s2 String, s3 String, s4 String,
    n1 Nullable(Int64), n2 Nullable(String),
    lc1 LowCardinality(String), lc2 LowCardinality(Nullable(String)),
    a1 Array(UInt32), a2 Array(String),
    tup Tuple(x UInt8, y String),
    m Map(String, UInt64),
    f1 Float64, f2 Float32, d Decimal(18, 4), dt Date,
    i1 Int8, i2 Int16, i3 Int32, i4 Int64, i5 UInt8, i6 UInt16, i7 UInt32, i8 UInt64,
    j1 Int8, j2 Int16, j3 Int32, j4 Int64, j5 UInt8, j6 UInt16, j7 UInt32, j8 UInt64
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_lazy_column_groups;

INSERT INTO t_lazy_column_groups SELECT
    number, toDateTime(1577836800 + intHash32(number) % 100000),
    repeat('a', number % 50), toString(number), concat('x', toString(number % 7)), '',
    if(number % 3 = 0, NULL, number), if(number % 5 = 0, NULL, toString(number)),
    toString(number % 10), if(number % 4 = 0, NULL, toString(number % 3)),
    range(number % 5), arrayMap(x -> toString(x), range(number % 3)),
    (number % 256, toString(number)),
    map(toString(number % 3), number),
    number / 3, number / 7, number / 11, toDate('2020-01-01') + number % 365,
    number, number, number, number, number, number, number, number,
    -number, -number, -number, -number, number * 3, number * 3, number * 3, number * 3
FROM numbers(20000);

INSERT INTO t_lazy_column_groups SELECT
    number + 20000, toDateTime(1577836800 + intHash32(number + 20000) % 100000),
    repeat('b', number % 40), toString(number), 'y', 'z',
    number, NULL, 'p', NULL, [1, 2, 3], ['q'], (1, 'w'), map('k', 1),
    1.5, 2.5, 3.5, toDate('2021-01-01'),
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
FROM numbers(20000);

CREATE TABLE t_lazy_column_groups_r (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_lazy_column_groups_r SELECT number * 2, toString(number) FROM numbers(20000);

SELECT 'limit 10';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, use_top_k_dynamic_filtering = 0,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = 'limit_10:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, use_top_k_dynamic_filtering = 0,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = 'limit_10:8';

SELECT 'limit 1 with filter';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups WHERE s3 = 'x3' ORDER BY t DESC, k LIMIT 1)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'limit_1_filter:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups WHERE s3 = 'x3' ORDER BY t DESC, k LIMIT 1)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'limit_1_filter:8';

SELECT 'limit 300 with virtual columns';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT _part, _part_offset, * FROM t_lazy_column_groups ORDER BY t, k LIMIT 300)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'limit_300_virtual:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT _part, _part_offset, * FROM t_lazy_column_groups ORDER BY t, k LIMIT 300)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'limit_300_virtual:8';

SELECT 'limit 5000';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups ORDER BY i8 DESC, k LIMIT 5000)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'limit_5000:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups ORDER BY i8 DESC, k LIMIT 5000)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'limit_5000:8';

SELECT 'without the sorting columns';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count()
FROM (SELECT s1, s2, s3, s4, n1, n2, lc1, lc2, a1, a2, tup, m, f1, f2, d, dt, i1, i2 FROM t_lazy_column_groups ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'no_sorting_columns:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count()
FROM (SELECT s1, s2, s3, s4, n1, n2, lc1, lc2, a1, a2, tup, m, f1, f2, d, dt, i1, i2 FROM t_lazy_column_groups ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'no_sorting_columns:8';

SELECT 'join';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count()
FROM (SELECT l.*, r.v FROM t_lazy_column_groups AS l JOIN t_lazy_column_groups_r AS r ON l.k = r.k ORDER BY l.t, l.k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'join:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count()
FROM (SELECT l.*, r.v FROM t_lazy_column_groups AS l JOIN t_lazy_column_groups_r AS r ON l.k = r.k ORDER BY l.t, l.k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'join:8';

SELECT 'one thread';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, max_threads = 1, log_comment = 'one_thread:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, max_threads = 1, log_comment = 'one_thread:8';

-- Each reader fills a column a part lacks from its default on its own: the older parts lack n and s, so no split.
CREATE TABLE t_lazy_column_groups_default
(
    k UInt64,
    t DateTime,
    c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8,
    c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8, c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8, c20 UInt8
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_lazy_column_groups_default;
INSERT INTO t_lazy_column_groups_default (k, t) SELECT number, toDateTime(1577836800 + intHash32(number) % 100000) FROM numbers(20000);
INSERT INTO t_lazy_column_groups_default (k, t) SELECT number + 20000, toDateTime(1577836800 + intHash32(number + 20000) % 100000) FROM numbers(20000);
ALTER TABLE t_lazy_column_groups_default ADD COLUMN n UInt64 DEFAULT rand64(), ADD COLUMN s String DEFAULT toString(n);
-- These parts store n and s.
INSERT INTO t_lazy_column_groups_default (k, t) SELECT number + 40000, toDateTime(1577836800 + intHash32(number + 40000) % 100000) FROM numbers(20000);
INSERT INTO t_lazy_column_groups_default (k, t) SELECT number + 60000, toDateTime(1577836800 + intHash32(number + 60000) % 100000) FROM numbers(20000);

-- NOT ignore(*) keeps every column of the subquery read.
SELECT 'default of a column a part lacks';
SELECT countIf(n != toUInt64(s)), countIf(k < 40000) > 0, countIf(k >= 40000) > 0, count()
FROM (SELECT * FROM t_lazy_column_groups_default ORDER BY t, k LIMIT 100) WHERE NOT ignore(*)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'missing_column:1';
SELECT countIf(n != toUInt64(s)), countIf(k < 40000) > 0, countIf(k >= 40000) > 0, count()
FROM (SELECT * FROM t_lazy_column_groups_default ORDER BY t, k LIMIT 100) WHERE NOT ignore(*)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'missing_column:8';
-- Only the parts that store every column are read.
SELECT countIf(n != toUInt64(s)), count()
FROM (SELECT * FROM t_lazy_column_groups_default WHERE k >= 40000 ORDER BY t, k LIMIT 100) WHERE NOT ignore(*)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'stored_columns:1';
SELECT countIf(n != toUInt64(s)), count()
FROM (SELECT * FROM t_lazy_column_groups_default WHERE k >= 40000 ORDER BY t, k LIMIT 100) WHERE NOT ignore(*)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'stored_columns:8';

-- The columns of a compact part are read from one file: no split.
CREATE TABLE t_lazy_column_groups_compact AS t_lazy_column_groups
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
SYSTEM STOP MERGES t_lazy_column_groups_compact;
INSERT INTO t_lazy_column_groups_compact SELECT * FROM t_lazy_column_groups WHERE k < 20000;
INSERT INTO t_lazy_column_groups_compact SELECT * FROM t_lazy_column_groups WHERE k >= 20000;

SELECT 'compact parts';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups_compact ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'compact:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups_compact ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'compact:8';

-- Lightweight delete: the readers of a range apply the same mask.
CREATE TABLE t_lazy_column_groups_delete AS t_lazy_column_groups
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_lazy_column_groups_delete;
INSERT INTO t_lazy_column_groups_delete SELECT * FROM t_lazy_column_groups WHERE k < 20000;
INSERT INTO t_lazy_column_groups_delete SELECT * FROM t_lazy_column_groups WHERE k >= 20000;
SYSTEM START MERGES t_lazy_column_groups_delete;
DELETE FROM t_lazy_column_groups_delete WHERE k % 11 = 0 SETTINGS mutations_sync = 2;

SELECT 'lightweight delete';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count(), countIf(k % 11 = 0) FROM (SELECT * FROM t_lazy_column_groups_delete ORDER BY t, k LIMIT 300)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'delete:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count(), countIf(k % 11 = 0) FROM (SELECT * FROM t_lazy_column_groups_delete ORDER BY t, k LIMIT 300)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'delete:8';

-- The main read is served by a projection, and the lazy read reads the projection parts.
CREATE TABLE t_lazy_column_groups_projection
(
    k UInt64, id String, payload String,
    c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8,
    c10 UInt8, c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8, c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8,
    PROJECTION p (SELECT * ORDER BY id)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_lazy_column_groups_projection;
INSERT INTO t_lazy_column_groups_projection (k, id, payload, c1, c18)
    SELECT number, concat('id', toString(number % 1000)), concat('payload', toString(number)), number % 200, number % 100 FROM numbers(10000);
INSERT INTO t_lazy_column_groups_projection (k, id, payload, c1, c18)
    SELECT number, concat('id', toString(number % 1000)), concat('payload', toString(number)), number % 200, number % 100 FROM numbers(10000, 10000);

SELECT 'projection';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups_projection WHERE id IN ('id5', 'id7') ORDER BY k DESC LIMIT 5)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, optimize_use_projections = 1, optimize_read_in_order = 0, optimize_move_to_prewhere = 0,
    log_comment = 'projection:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups_projection WHERE id IN ('id5', 'id7') ORDER BY k DESC LIMIT 5)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, optimize_use_projections = 1, optimize_read_in_order = 0, optimize_move_to_prewhere = 0,
    log_comment = 'projection:8';

-- A pending DROP COLUMN leaves the data in the parts, and the reader computes the default of the column added back:
-- no split while a mutation is pending. big is the largest column, so m and m.keys would be read in different groups.
CREATE TABLE t_lazy_column_groups_pending
(
    k UInt64,
    t DateTime,
    big String,
    c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8,
    c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8, c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8, c20 UInt8,
    m Map(String, UInt64)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_lazy_column_groups_pending;
INSERT INTO t_lazy_column_groups_pending (k, t, big, m)
    SELECT number, toDateTime(1577836800 + intHash32(number) % 100000), repeat('x', 100), map('a', number) FROM numbers(20000);
INSERT INTO t_lazy_column_groups_pending (k, t, big, m)
    SELECT number + 20000, toDateTime(1577836800 + intHash32(number + 20000) % 100000), repeat('x', 100), map('a', number) FROM numbers(20000);
ALTER TABLE t_lazy_column_groups_pending DROP COLUMN m SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_lazy_column_groups_pending ADD COLUMN m Map(String, UInt64) DEFAULT map(toString(rand64()), 1) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'pending mutation';
SELECT count() > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't_lazy_column_groups_pending' AND NOT is_done;
SELECT countIf(mk != mapKeys(m)), count() FROM (SELECT *, m.keys AS mk FROM t_lazy_column_groups_pending ORDER BY t, k LIMIT 100) WHERE NOT ignore(*)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'pending_mutation:1';
SELECT countIf(mk != mapKeys(m)), count() FROM (SELECT *, m.keys AS mk FROM t_lazy_column_groups_pending ORDER BY t, k LIMIT 100) WHERE NOT ignore(*)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'pending_mutation:8';

-- A finished mutation is still listed in system.mutations, but does not stop the split.
CREATE TABLE t_lazy_column_groups_finished AS t_lazy_column_groups_pending
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_column_groups_finished (k, t, big, m)
    SELECT number, toDateTime(1577836800 + intHash32(number) % 100000), repeat('x', 100), map('a', number) FROM numbers(20000);
INSERT INTO t_lazy_column_groups_finished (k, t, big, m)
    SELECT number + 20000, toDateTime(1577836800 + intHash32(number + 20000) % 100000), repeat('x', 100), map('a', number) FROM numbers(20000);
ALTER TABLE t_lazy_column_groups_finished DROP COLUMN c20 SETTINGS mutations_sync = 2;

SELECT 'finished mutation';
SELECT count() > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't_lazy_column_groups_finished' AND is_done;
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups_finished ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 1, log_comment = 'finished_mutation:1';
SELECT groupBitXor(cityHash64(formatRow('TSV', *))), count() FROM (SELECT * FROM t_lazy_column_groups_finished ORDER BY t, k LIMIT 10)
SETTINGS query_plan_max_column_groups_for_lazy_materialization = 8, log_comment = 'finished_mutation:8';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The readers of the lazy read: more with the setting at 8 where the columns are split, as many where they are not.
SELECT 'readers';
SELECT query, multiIf(readers_8 > readers_1, 'more', readers_8 = readers_1, 'same', 'fewer')
FROM
(
    SELECT
        splitByChar(':', q.log_comment)[1] AS query,
        countIf(endsWith(q.log_comment, ':8')) AS readers_8,
        countIf(endsWith(q.log_comment, ':1')) AS readers_1
    FROM system.query_log AS q JOIN system.processors_profile_log AS p ON p.query_id = q.query_id
    WHERE q.current_database = currentDatabase() AND q.type = 'QueryFinish' AND q.event_date >= yesterday() AND q.log_comment LIKE '%:_'
        AND p.plan_step_name = 'LazilyReadFromMergeTree' AND startsWith(p.name, 'MergeTreeSelect')
    GROUP BY query
)
ORDER BY query;

-- The readers of the other column groups do not count the rows they read.
SELECT 'read rows';
SELECT
    anyIf(read_rows, log_comment = 'limit_10:8') = anyIf(read_rows, log_comment = 'limit_10:1'),
    anyIf(read_rows, log_comment = 'limit_10:1') > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment IN ('limit_10:1', 'limit_10:8');

SELECT 'projection used';
SELECT log_comment, notEmpty(projections) FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment LIKE 'projection:_'
ORDER BY log_comment;

DROP TABLE t_lazy_column_groups;
DROP TABLE t_lazy_column_groups_r;
DROP TABLE t_lazy_column_groups_default;
DROP TABLE t_lazy_column_groups_compact;
DROP TABLE t_lazy_column_groups_delete;
DROP TABLE t_lazy_column_groups_projection;
DROP TABLE t_lazy_column_groups_pending;
DROP TABLE t_lazy_column_groups_finished;
