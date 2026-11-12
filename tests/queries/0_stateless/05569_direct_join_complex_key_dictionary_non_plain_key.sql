-- Tags: no-parallel-replicas
-- DirectKeyValueJoin cannot be chosen with parallel replicas.

DROP DICTIONARY IF EXISTS dict_direct;
DROP DICTIONARY IF EXISTS dict_cache;
DROP DICTIONARY IF EXISTS dict_direct_u64;
DROP TABLE IF EXISTS dict_source;
DROP TABLE IF EXISTS dict_source_u64;
DROP TABLE IF EXISTS probe_arr;
DROP TABLE IF EXISTS probe_sparse;

CREATE TABLE dict_source (k String, v UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO dict_source VALUES ('a', 7), ('b', 8);

CREATE TABLE dict_source_u64 (k UInt64, v UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO dict_source_u64 VALUES (0, 5), (3, 6);

CREATE DICTIONARY dict_direct (k String, v UInt8) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase())) LAYOUT(COMPLEX_KEY_DIRECT());

-- LIFETIME 0: every lookup is a cache miss that loads keys from the source.
CREATE DICTIONARY dict_cache (k String, v UInt8) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase())) LIFETIME(MIN 0 MAX 0) LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 16));

CREATE DICTIONARY dict_direct_u64 (k UInt64, v UInt8) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'dict_source_u64' DB currentDatabase())) LAYOUT(COMPLEX_KEY_DIRECT());

CREATE TABLE probe_arr (k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO probe_arr VALUES ('a'), ('c');

CREATE TABLE probe_sparse (n UInt64, k UInt64) ENGINE = MergeTree ORDER BY n
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO probe_sparse SELECT number, if(number = 3, 3, 0) FROM numbers(100);

-- The sparse arm is only meaningful if `k` is stored as Sparse.
SELECT 'probe_sparse.k is Sparse', countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'probe_sparse' AND column = 'k' AND active;

SET join_algorithm = 'direct'; -- the path under test
SET join_use_nulls = 0; -- a miss prints 0
SET enable_lazy_columns_replication = 1; -- keys after ARRAY JOIN are ColumnReplicated

SELECT 'Const key';
SELECT l.n, d.v FROM (SELECT number AS n, 'a' AS k FROM numbers(2)) AS l INNER JOIN dict_direct AS d ON l.k = d.k ORDER BY l.n;
SELECT l.n, d.v FROM (SELECT number AS n, 'c' AS k FROM numbers(2)) AS l LEFT JOIN dict_direct AS d ON l.k = d.k ORDER BY l.n;
SELECT l.n, d.v FROM (SELECT number AS n, 'b' AS k FROM numbers(2)) AS l INNER JOIN dict_cache AS d ON l.k = d.k ORDER BY l.n;
SELECT l.n, d.v FROM (SELECT number AS n, toUInt64(3) AS k FROM numbers(2)) AS l INNER JOIN dict_direct_u64 AS d ON l.k = d.k ORDER BY l.n;

SELECT 'LowCardinality const key';
SELECT l.n, d.v FROM (SELECT number AS n, toLowCardinality('a') AS k FROM numbers(2)) AS l INNER JOIN dict_direct AS d ON l.k = d.k ORDER BY l.n;

SELECT 'Replicated key after ARRAY JOIN';
SELECT l.k, l.x, d.v FROM (SELECT k, x FROM probe_arr ARRAY JOIN range(2) AS x) AS l LEFT JOIN dict_direct AS d ON l.k = d.k ORDER BY l.k, l.x;
SELECT l.k, l.x, d.v FROM (SELECT k, x FROM probe_arr ARRAY JOIN range(2) AS x) AS l INNER JOIN dict_cache AS d ON l.k = d.k ORDER BY l.k, l.x;

SELECT 'Sparse key';
SELECT count(), sum(d.v), countIf(d.v = 6) FROM probe_sparse AS l INNER JOIN dict_direct_u64 AS d ON l.k = d.k;

DROP DICTIONARY dict_direct;
DROP DICTIONARY dict_cache;
DROP DICTIONARY dict_direct_u64;
DROP TABLE dict_source;
DROP TABLE dict_source_u64;
DROP TABLE probe_arr;
DROP TABLE probe_sparse;
