-- A `bloom_filter` index on a table column literally named `mapKeys(m)` or `mapValues(m)`
-- must not be used for predicates over the `Map` column `m`.

DROP TABLE IF EXISTS t_bf_fixed;
DROP TABLE IF EXISTS t_bf_lc;
DROP TABLE IF EXISTS t_bf_real;

CREATE TABLE t_bf_fixed
(
    m Map(String, String),
    `mapKeys(m)` FixedString(16),
    INDEX idx `mapKeys(m)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_fixed VALUES (map('a', 'x'), 'deadbeefdeadbeef');
INSERT INTO t_bf_fixed VALUES (map('k', 'v'), 'deadbeefdeadbeef');

SELECT count() FROM t_bf_fixed WHERE m['k'] = 'v';
SELECT count() FROM t_bf_fixed WHERE m['k'] = 'v' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bf_fixed WHERE mapContainsKey(m, 'k');
SELECT count() FROM t_bf_fixed WHERE has(m, 'k');

CREATE TABLE t_bf_lc
(
    m Map(String, String),
    `mapKeys(m)` LowCardinality(String),
    `mapValues(m)` LowCardinality(String),
    INDEX idx_k `mapKeys(m)` TYPE bloom_filter,
    INDEX idx_v `mapValues(m)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_lc VALUES (map('k', 'v'), 'x', 'y');

SELECT count() FROM t_bf_lc WHERE m['k'] = 'v';
SELECT count() FROM t_bf_lc WHERE m['k'] = 'v' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bf_lc WHERE mapContainsKey(m, 'k');
SELECT count() FROM t_bf_lc WHERE mapContainsValue(m, 'v');
SELECT count() FROM t_bf_lc WHERE has(m, 'k');

-- A real index on the `mapKeys(m)` expression is still used.
CREATE TABLE t_bf_real
(
    m Map(String, String),
    INDEX idx mapKeys(m) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_real VALUES (map('k', 'v'));

SELECT count() FROM t_bf_real WHERE m['k'] = 'v' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_bf_real WHERE mapContainsKey(m, 'k') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_bf_fixed;
DROP TABLE t_bf_lc;
DROP TABLE t_bf_real;
