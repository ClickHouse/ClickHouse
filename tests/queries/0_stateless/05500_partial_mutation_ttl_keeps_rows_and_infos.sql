-- A mutation that rewrites only some columns of a Wide part applies the column TTLs of the columns it
-- writes, never changes the number of rows, and keeps the TTL info of every rule it does not apply.
-- Every table is created with `min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0`:
-- a Compact or packed part is rewritten whole by every mutation.

-- T1: rows TTL whose input is read, half of the rows expired.
CREATE TABLE t1 (k UInt64, d1 DateTime('UTC'), d3 DateTime('UTC'), w String TTL greatest(d1, d3) + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k TTL d3 + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t1;
INSERT INTO t1 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'),
    if(number % 2 = 0, toDateTime('2020-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC')), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T1 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't1' AND active;
ALTER TABLE t1 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T1', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')), countIf(w = toString(k)), countIf(v = toString(k)) FROM t1;
SELECT 'T1 info', delete_ttl_info_min = toDateTime('2020-01-01 00:00:01', 'UTC'), delete_ttl_info_max = toDateTime('2090-01-01 00:00:01', 'UTC')
FROM system.parts WHERE database = currentDatabase() AND table = 't1' AND active;
SYSTEM START TTL MERGES t1;
OPTIMIZE TABLE t1 FINAL;
SELECT 'T1 merged', count(), countIf(d3 = toDateTime('2090-01-01 00:00:00', 'UTC')) FROM t1;

-- T2: every row expired.
CREATE TABLE t2 (k UInt64, d1 DateTime('UTC'), d3 DateTime('UTC'), w String TTL d1 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k TTL d3 + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t2;
INSERT INTO t2 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2020-01-01 00:00:00', 'UTC'), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T2 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't2' AND active;
ALTER TABLE t2 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T2', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')), countIf(w = toString(k)) FROM t2;
SYSTEM START TTL MERGES t2;
OPTIMIZE TABLE t2 FINAL;
SELECT 'T2 merged', count() FROM t2;

-- T3: rows TTL on a column the mutation does not read.
CREATE TABLE t3 (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), w String TTL d1 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k TTL d2 + INTERVAL 10 YEAR
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t3;
INSERT INTO t3 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC'), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T3 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't3' AND active;
ALTER TABLE t3 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T3 mutation', is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't3';
SELECT 'T3', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')) FROM t3;
SELECT 'T3 info', delete_ttl_info_min = toDateTime('2100-01-01 00:00:00', 'UTC'), delete_ttl_info_max = toDateTime('2100-01-01 00:00:00', 'UTC')
FROM system.parts WHERE database = currentDatabase() AND table = 't3' AND active;

-- T4: rows `DELETE WHERE` TTL whose inputs are read.
CREATE TABLE t4 (k UInt64, d1 DateTime('UTC'), d3 DateTime('UTC'), w String TTL greatest(d1, d3) + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k TTL d3 + INTERVAL 1 SECOND DELETE WHERE d3 > 0
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t4;
INSERT INTO t4 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'),
    if(number % 2 = 0, toDateTime('2020-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC')), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T4 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't4' AND active;
ALTER TABLE t4 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T4', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')), countIf(w = toString(k)), countIf(v = toString(k)) FROM t4;
SELECT 'T4 info', rows_where_ttl_info.min = [toDateTime('2020-01-01 00:00:01', 'UTC')], rows_where_ttl_info.max = [toDateTime('2090-01-01 00:00:01', 'UTC')]
FROM system.parts WHERE database = currentDatabase() AND table = 't4' AND active;
SYSTEM START TTL MERGES t4;
OPTIMIZE TABLE t4 FINAL;
SELECT 'T4 merged', count(), countIf(d3 = toDateTime('2090-01-01 00:00:00', 'UTC')) FROM t4;

-- T5: `GROUP BY` TTL whose inputs are read.
CREATE TABLE t5 (k UInt64, d1 DateTime('UTC'), d3 DateTime('UTC'), w String TTL greatest(d1, d3) + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY (k, d3) TTL d3 + INTERVAL 1 SECOND GROUP BY k SET v = any(v)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t5;
INSERT INTO t5 SELECT number % 10, toDateTime('2090-01-01 00:00:00', 'UTC'),
    if(number % 2 = 0, toDateTime('2020-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC')), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T5 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't5' AND active;
ALTER TABLE t5 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T5', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')), countIf(v = w) FROM t5;
SELECT 'T5 info', group_by_ttl_info.min = [toDateTime('2020-01-01 00:00:01', 'UTC')], group_by_ttl_info.max = [toDateTime('2090-01-01 00:00:01', 'UTC')]
FROM system.parts WHERE database = currentDatabase() AND table = 't5' AND active;

-- T6: `RECOMPRESS` TTL on a column the mutation does not read.
CREATE TABLE t6 (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), w String TTL d1 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k TTL d2 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(5))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t6;
INSERT INTO t6 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC'), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T6 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't6' AND active;
ALTER TABLE t6 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T6', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')) FROM t6;
SELECT 'T6 info', recompression_ttl_info.min = [toDateTime('2090-01-02 00:00:00', 'UTC')], recompression_ttl_info.max = [toDateTime('2090-01-02 00:00:00', 'UTC')]
FROM system.parts WHERE database = currentDatabase() AND table = 't6' AND active;
SELECT 'T6 w', column_ttl_min = toDateTime('2105-01-01 00:00:00', 'UTC') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't6' AND active AND column = 'w';

-- T7: expired column TTL of a column the mutation does not rewrite.
CREATE TABLE t7 (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), w String TTL d1 + INTERVAL 10 YEAR, x String TTL d2 + INTERVAL 1 SECOND, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t7;
INSERT INTO t7 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2020-01-01 00:00:00', 'UTC'), toString(number), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T7 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't7' AND active;
ALTER TABLE t7 UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T7 x', column_ttl_min = toDateTime('2020-01-01 00:00:01', 'UTC'), column_ttl_max = toDateTime('2020-01-01 00:00:01', 'UTC')
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't7' AND active AND column = 'x';
SYSTEM START TTL MERGES t7;
OPTIMIZE TABLE t7 FINAL;
SELECT 'T7 merged', count(), countIf(x != ''), countIf(w = toString(k)) FROM t7;

-- T8: the mutation writes a column whose TTL reads a column the mutation does not otherwise read.
CREATE TABLE t8 (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), w String TTL d2 + INTERVAL 10 YEAR, y String TTL d1 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t8;
INSERT INTO t8 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC'), toString(number), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T8 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't8' AND active;
ALTER TABLE t8 UPDATE w = 'x', d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T8', count(), countIf(w = 'x'), countIf(y = toString(k)) FROM t8;
SELECT 'T8 ttl', column, column_ttl_min = column_ttl_max,
    column_ttl_min = if(column = 'w', toDateTime('2100-01-01 00:00:00', 'UTC'), toDateTime('2105-01-01 00:00:00', 'UTC'))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't8' AND active AND column IN ('w', 'y') ORDER BY column;

-- T9: the column TTL resets the rewritten column to its `DEFAULT`, which reads a column the mutation does not otherwise read.
CREATE TABLE t9 (d DateTime('UTC'), k UInt32, w Int32 DEFAULT k + 1000 TTL d + INTERVAL 1 DAY, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t9;
INSERT INTO t9 (d, k, w, v) SELECT toDateTime('2090-01-01 00:00:00', 'UTC'), number, number, toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T9 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't9' AND active;
ALTER TABLE t9 UPDATE d = toDateTime('2020-01-01 00:00:00', 'UTC') WHERE k % 2 = 1 SETTINGS mutations_sync = 2;
SELECT 'T9', count(), countIf(k % 2 = 0 AND w = k), countIf(k % 2 = 1 AND w = k + 1000) FROM t9;
SELECT 'T9 w', column_ttl_min = toDateTime('2090-01-02 00:00:00', 'UTC') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't9' AND active AND column = 'w';

-- T10: the rewritten column's TTL was fully applied by a merge before.
CREATE TABLE t10 (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), w String TTL d2 + INTERVAL 1 SECOND, y String TTL d1 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t10 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2020-01-01 00:00:00', 'UTC'), toString(number), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
OPTIMIZE TABLE t10 FINAL;
SYSTEM STOP TTL MERGES t10;
SELECT 'T10 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't10' AND active;
SELECT 'T10 before', countIf(w = '') FROM t10;
ALTER TABLE t10 UPDATE w = 'x', d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T10', count(), countIf(d1 = toDateTime('2095-01-01 00:00:00', 'UTC')), countIf(w = ''), countIf(y = toString(k)) FROM t10;

-- T11: the TTL of the rewritten column reads a subcolumn.
CREATE TABLE t11 (k UInt64, d1 DateTime('UTC'), t Tuple(d DateTime('UTC')), w String TTL t.d + INTERVAL 10 YEAR, y String TTL d1 + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t11;
INSERT INTO t11 SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), tuple(toDateTime('2090-01-01 00:00:00', 'UTC')), toString(number), toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T11 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't11' AND active;
ALTER TABLE t11 UPDATE w = 'x', d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T11', count(), countIf(w = 'x') FROM t11;
SELECT 'T11 w', column_ttl_min = toDateTime('2100-01-01 00:00:00', 'UTC') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't11' AND active AND column = 'w';

-- T12: the mutation moves the input of an expired column TTL into the future.
CREATE TABLE t12 (k UInt64, d DateTime('UTC'), w String TTL d + INTERVAL 1 SECOND, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t12;
INSERT INTO t12 SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC'), 'a' || toString(number), toString(number)
FROM numbers(100) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
SELECT 'T12 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't12' AND active;
ALTER TABLE t12 UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'T12', count(), countIf(w = 'a' || toString(k)) FROM t12;
SELECT 'T12 w', column_ttl_min = toDateTime('2090-01-01 00:00:01', 'UTC') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't12' AND active AND column = 'w';

-- T13: CLEAR COLUMN recomputes a MATERIALIZED column whose own TTL reads a column the mutation does not otherwise read.
CREATE TABLE t13 (k UInt64, c DateTime('UTC'), e DateTime('UTC'), x String TTL c + INTERVAL 10 YEAR,
    m DateTime('UTC') MATERIALIZED c TTL e + INTERVAL 10 YEAR, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t13;
INSERT INTO t13 (k, c, e, x, v) SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC'),
    toString(number), toString(number) FROM numbers(100)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_block_size = 1000;
ALTER TABLE t13 CLEAR COLUMN c SETTINGS mutations_sync = 2;
SELECT 'T13 mutation', is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't13';
SELECT 'T13', count(), countIf(m = toDateTime(0, 'UTC')), countIf(x = toString(k)) FROM t13;

-- Every mutation above rewrote only some columns of its part.
SYSTEM FLUSH LOGS part_log;
SELECT 'partial', table, ProfileEvents['MutationSomePartColumns'] > 0, ProfileEvents['MutationAllPartColumns'] = 0
FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND event_type = 'MutatePart'
ORDER BY toUInt32(substring(table, 2));
