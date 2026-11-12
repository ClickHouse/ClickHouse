-- The memory of the index of one hypothetical projection is released before the next one is estimated.

DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt64, b String, v UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';

INSERT INTO t SELECT number, leftPad(toString(cityHash64(number)), 10000, '0'), number FROM numbers(1000);

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0;

-- The index takes about 10 MB.
CREATE HYPOTHETICAL PROJECTION p1 ON t (SELECT a, b, v ORDER BY (b, v + 1));
CREATE HYPOTHETICAL PROJECTION p2 ON t (SELECT a, b, v ORDER BY (b, v + 2));
CREATE HYPOTHETICAL PROJECTION p3 ON t (SELECT a, b, v ORDER BY (b, v + 3));
CREATE HYPOTHETICAL PROJECTION p4 ON t (SELECT a, b, v ORDER BY (b, v + 4));
CREATE HYPOTHETICAL PROJECTION p5 ON t (SELECT a, b, v ORDER BY (b, v + 5));
CREATE HYPOTHETICAL PROJECTION p6 ON t (SELECT a, b, v ORDER BY (b, v + 6));

SELECT count() FROM (EXPLAIN WHATIF SELECT a, b, v FROM t WHERE b = 'x')
WHERE match(explain, '^\\s*status:\\s*applicable')
SETTINGS max_memory_usage = '70M';

DROP TABLE t;
