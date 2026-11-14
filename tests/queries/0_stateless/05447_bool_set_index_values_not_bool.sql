-- Index analysis must not read a set element other than 0 or 1 as a `Bool` key value.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_nb_part;
DROP TABLE IF EXISTS t_lcnb_part;
DROP TABLE IF EXISTS t_nb_skip;
DROP TABLE IF EXISTS t_bool_tostring;

-- One part per value, so each part's key range is a single point.
CREATE TABLE t_nb_part (b Nullable(Bool)) ENGINE = MergeTree PARTITION BY b ORDER BY b SETTINGS allow_nullable_key = 1;
INSERT INTO t_nb_part VALUES (true);
INSERT INTO t_nb_part VALUES (false);

CREATE TABLE t_lcnb_part (b LowCardinality(Nullable(Bool))) ENGINE = MergeTree PARTITION BY b ORDER BY b SETTINGS allow_nullable_key = 1;
INSERT INTO t_lcnb_part VALUES (true);
INSERT INTO t_lcnb_part VALUES (false);

CREATE TABLE t_nb_skip (k UInt8, b Nullable(Bool), INDEX ib b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_nb_skip VALUES (1, true), (2, false);

CREATE TABLE t_bool_tostring (b Bool) ENGINE = MergeTree ORDER BY toString(b);
INSERT INTO t_bool_tostring VALUES (true);
INSERT INTO t_bool_tostring VALUES (false);

SELECT 'nb literal', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE b NOT IN (10) ORDER BY b);
SELECT 'nb global', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE b GLOBAL NOT IN (NULL, 1024, 10) ORDER BY b);
SELECT 'nb 0,10', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE b NOT IN (0, 10) ORDER BY b);
SELECT 'nb array', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE b NOT IN [255] ORDER BY b);
SELECT 'nb subquery', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE b NOT IN (SELECT 10) ORDER BY b);
SELECT 'nb has', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE NOT has([10], b) ORDER BY b) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT 'nb in', groupArray(b) FROM (SELECT b FROM t_nb_part WHERE b IN (1, 10) ORDER BY b);
SELECT 'nb tlp', count() FROM (
    SELECT DISTINCT b FROM t_nb_part WHERE b GLOBAL IN (NULL, 1024, 10)
    UNION DISTINCT SELECT DISTINCT b FROM t_nb_part WHERE NOT (b GLOBAL IN (NULL, 1024, 10))
    UNION DISTINCT SELECT DISTINCT b FROM t_nb_part WHERE isNull(b GLOBAL IN (NULL, 1024, 10)));
SELECT 'lcnb literal', groupArray(b) FROM (SELECT b FROM t_lcnb_part WHERE b NOT IN (10) ORDER BY b);
SELECT 'skip literal', groupArray(b) FROM (SELECT b FROM t_nb_skip WHERE b NOT IN (10) ORDER BY b);
SELECT 'skip subquery', groupArray(b) FROM (SELECT b FROM t_nb_skip WHERE b NOT IN (SELECT 10) ORDER BY b);
SELECT 'tostring literal', groupArray(b) FROM (SELECT b FROM t_bool_tostring WHERE b NOT IN (2) ORDER BY b);

DROP TABLE t_nb_part;
DROP TABLE t_lcnb_part;
DROP TABLE t_nb_skip;
DROP TABLE t_bool_tostring;
