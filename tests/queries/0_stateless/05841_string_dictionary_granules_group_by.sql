-- `GROUP BY` over `String` columns with dictionary granules equals `GROUP BY` over a `with_size_stream` twin: a lone
-- `count`, other aggregates, no aggregates, and `min`/`max` of the column that the key is computed from. Each runs in the
-- ordinary kernel (adaptive aggregator off) and in the frozen kernel of the adaptive aggregator (frozen at 10 keys, no
-- thaw). Both look the key of each dictionary position up once, because the values repeat within a block.

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_dict;

CREATE TABLE t_plain (id UInt64, k String, v String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream';

CREATE TABLE t_dict (id UInt64, k String, v String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types',
    string_serialization_version = 'with_dictionaries';

SYSTEM STOP MERGES t_plain;
SYSTEM STOP MERGES t_dict;

-- Four parts, so that four threads read them.
INSERT INTO t_plain SELECT number, 'key_' || toString(number % 97), 'domain_' || toString(number % 13) || '/page_' || toString(number % 7) FROM numbers(0, 10000);
INSERT INTO t_plain SELECT number, 'key_' || toString(number % 97), 'domain_' || toString(number % 13) || '/page_' || toString(number % 7) FROM numbers(10000, 10000);
INSERT INTO t_plain SELECT number, 'key_' || toString(number % 97), 'domain_' || toString(number % 13) || '/page_' || toString(number % 7) FROM numbers(20000, 10000);
INSERT INTO t_plain SELECT number, 'key_' || toString(number % 97), 'domain_' || toString(number % 13) || '/page_' || toString(number % 7) FROM numbers(30000, 10000);
INSERT INTO t_dict SELECT * FROM t_plain WHERE id < 10000;
INSERT INTO t_dict SELECT * FROM t_plain WHERE id >= 10000 AND id < 20000;
INSERT INTO t_dict SELECT * FROM t_plain WHERE id >= 20000 AND id < 30000;
INSERT INTO t_dict SELECT * FROM t_plain WHERE id >= 30000;

SELECT 'ordinary count', (SELECT sum(cityHash64(k, c)) FROM (SELECT k, count() AS c FROM t_plain GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4)
    = (SELECT sum(cityHash64(k, c)) FROM (SELECT k, count() AS c FROM t_dict GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4);
SELECT 'ordinary arguments', (SELECT sum(cityHash64(k, u, l)) FROM (SELECT k, uniqExact(id) AS u, sum(length(v)) AS l FROM t_plain GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4)
    = (SELECT sum(cityHash64(k, u, l)) FROM (SELECT k, uniqExact(id) AS u, sum(length(v)) AS l FROM t_dict GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4);
SELECT 'ordinary key only', (SELECT sum(cityHash64(k)) FROM (SELECT k FROM t_plain GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4)
    = (SELECT sum(cityHash64(k)) FROM (SELECT k FROM t_dict GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4);
SELECT 'ordinary min max', (SELECT sum(cityHash64(p, a, b, c)) FROM (SELECT splitByChar('/', v)[1] AS p, min(v) AS a, max(v) AS b, count() AS c FROM t_plain GROUP BY p) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4)
    = (SELECT sum(cityHash64(p, a, b, c)) FROM (SELECT splitByChar('/', v)[1] AS p, min(v) AS a, max(v) AS b, count() AS c FROM t_dict GROUP BY p) SETTINGS enable_adaptive_aggregator = 0, max_threads = 4);

SELECT 'frozen count', (SELECT sum(cityHash64(k, c)) FROM (SELECT k, count() AS c FROM t_plain GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4)
    = (SELECT sum(cityHash64(k, c)) FROM (SELECT k, count() AS c FROM t_dict GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4);
SELECT 'frozen arguments', (SELECT sum(cityHash64(k, u, l)) FROM (SELECT k, uniqExact(id) AS u, sum(length(v)) AS l FROM t_plain GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4)
    = (SELECT sum(cityHash64(k, u, l)) FROM (SELECT k, uniqExact(id) AS u, sum(length(v)) AS l FROM t_dict GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4);
SELECT 'frozen key only', (SELECT sum(cityHash64(k)) FROM (SELECT k FROM t_plain GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4)
    = (SELECT sum(cityHash64(k)) FROM (SELECT k FROM t_dict GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4);
SELECT 'frozen min max', (SELECT sum(cityHash64(p, a, b, c)) FROM (SELECT splitByChar('/', v)[1] AS p, min(v) AS a, max(v) AS b, count() AS c FROM t_plain GROUP BY p) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4)
    = (SELECT sum(cityHash64(p, a, b, c)) FROM (SELECT splitByChar('/', v)[1] AS p, min(v) AS a, max(v) AS b, count() AS c FROM t_dict GROUP BY p) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 10, adaptive_aggregator_disable_thaw = 1, max_threads = 4);

DROP TABLE t_plain;
DROP TABLE t_dict;
