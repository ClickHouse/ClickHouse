-- A text, tokenbf_v1 or ngrambf_v1 index on a column named like a call of `blockSize` must not stand in for the call.
-- https://github.com/ClickHouse/ClickHouse/issues/134178

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;

-- Every row stores '0', while `toString(blockSize() > 0)` is '1'.
CREATE TABLE t_text (`toString(greater(blockSize(), 0))` String, id UInt64, INDEX r_idx `toString(greater(blockSize(), 0))` TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_text SELECT '0', number FROM numbers(8000);

CREATE TABLE t_tokenbf (`toString(greater(blockSize(), 0))` String, id UInt64, INDEX r_idx `toString(greater(blockSize(), 0))` TYPE tokenbf_v1(1024, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_tokenbf SELECT '0', number FROM numbers(8000);

CREATE TABLE t_ngrambf (`toString(greater(blockSize(), 0))` String, id UInt64, INDEX r_idx `toString(greater(blockSize(), 0))` TYPE ngrambf_v1(1, 1024, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_ngrambf SELECT '0', number FROM numbers(8000);

SELECT '-- text';
SELECT 'equals', count() FROM t_text WHERE toString(blockSize() > 0) = '1';
SELECT 'equals, direct read', count() FROM t_text WHERE toString(blockSize() > 0) = '1' SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT 'equals, no direct read', count() FROM t_text WHERE toString(blockSize() > 0) = '1' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasToken', count() FROM t_text WHERE hasToken(toString(blockSize() > 0), '1');
SELECT 'hasAnyTokens', count() FROM t_text WHERE hasAnyTokens(toString(blockSize() > 0), ['1']);
SELECT 'IN', count() FROM t_text WHERE toString(blockSize() > 0) IN ('1', '2');
SELECT 'control: column', count() FROM t_text WHERE `toString(greater(blockSize(), 0))` = '1';
SELECT 'control: index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE `toString(greater(blockSize(), 0))` = '1' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Name: r_idx%';

SELECT '-- tokenbf_v1';
SELECT 'equals', count() FROM t_tokenbf WHERE toString(blockSize() > 0) = '1';
SELECT 'hasToken', count() FROM t_tokenbf WHERE hasToken(toString(blockSize() > 0), '1');
SELECT 'IN', count() FROM t_tokenbf WHERE toString(blockSize() > 0) IN ('1', '2');
SELECT 'control: column', count() FROM t_tokenbf WHERE `toString(greater(blockSize(), 0))` = '1';
SELECT 'control: index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE `toString(greater(blockSize(), 0))` = '1' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Name: r_idx%';

SELECT '-- ngrambf_v1';
SELECT 'equals', count() FROM t_ngrambf WHERE toString(blockSize() > 0) = '1';
SELECT 'LIKE', count() FROM t_ngrambf WHERE toString(blockSize() > 0) LIKE '%1%';
SELECT 'IN', count() FROM t_ngrambf WHERE toString(blockSize() > 0) IN ('1', '2');
SELECT 'control: column', count() FROM t_ngrambf WHERE `toString(greater(blockSize(), 0))` = '1';
SELECT 'control: index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE `toString(greater(blockSize(), 0))` = '1' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Name: r_idx%';

DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
