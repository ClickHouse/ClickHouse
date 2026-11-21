-- `s = ''` is rewritten to `empty(s)`. The sorting-key prefix must still be fixed,
-- so `ORDER BY id` is read in order. `!= ''` does not fix `s`.

SET enable_analyzer = 1;
SET optimize_empty_string_comparisons = 1;
SET optimize_read_in_order = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_128595_rio_str;
DROP TABLE IF EXISTS t_128595_rio_lc;
DROP TABLE IF EXISTS t_128595_rio_null;
DROP TABLE IF EXISTS t_128595_rio_fs;

CREATE TABLE t_128595_rio_str (s String, id UInt64) ENGINE = MergeTree ORDER BY (s, id);
INSERT INTO t_128595_rio_str SELECT if(number % 2 = 0, '', toString(number)), number FROM numbers(6);

CREATE TABLE t_128595_rio_lc (s LowCardinality(String), id UInt64) ENGINE = MergeTree ORDER BY (s, id);
INSERT INTO t_128595_rio_lc SELECT if(number % 2 = 0, '', toString(number)), number FROM numbers(6);

CREATE TABLE t_128595_rio_null (s Nullable(String), id UInt64) ENGINE = MergeTree ORDER BY (s, id) SETTINGS allow_nullable_key = 1;
INSERT INTO t_128595_rio_null SELECT multiIf(number % 3 = 0, '', number % 3 = 1, 'a', CAST(NULL, 'Nullable(String)')), number FROM numbers(9);

CREATE TABLE t_128595_rio_fs (s FixedString(2), id UInt64) ENGINE = MergeTree ORDER BY (s, id);
INSERT INTO t_128595_rio_fs SELECT toFixedString(if(number % 2 = 0, '', 'a'), 2), number FROM numbers(6);

SELECT 'string in order';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT id FROM t_128595_rio_str WHERE s = '' ORDER BY id LIMIT 3) WHERE explain LIKE '%Read type: InOrder%';
SELECT id FROM t_128595_rio_str WHERE s = '' ORDER BY id LIMIT 3;

SELECT 'string not in order';
SELECT count() = 0 FROM (EXPLAIN PLAN SELECT id FROM t_128595_rio_str WHERE s != '' ORDER BY id LIMIT 3) WHERE explain LIKE '%Read type: InOrder%';

SELECT 'lc in order';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT id FROM t_128595_rio_lc WHERE s = '' ORDER BY id LIMIT 3) WHERE explain LIKE '%Read type: InOrder%';
SELECT id FROM t_128595_rio_lc WHERE s = '' ORDER BY id LIMIT 3;

SELECT 'nullable in order';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT id FROM t_128595_rio_null WHERE s = '' ORDER BY id LIMIT 3) WHERE explain LIKE '%Read type: InOrder%';
SELECT id FROM t_128595_rio_null WHERE s = '' ORDER BY id LIMIT 3;

SELECT 'fixed in order';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT id FROM t_128595_rio_fs WHERE s = '' ORDER BY id LIMIT 3) WHERE explain LIKE '%Read type: InOrder%';
SELECT id FROM t_128595_rio_fs WHERE s = '' ORDER BY id LIMIT 3;

DROP TABLE t_128595_rio_str;
DROP TABLE t_128595_rio_lc;
DROP TABLE t_128595_rio_null;
DROP TABLE t_128595_rio_fs;
