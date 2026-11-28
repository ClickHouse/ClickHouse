-- Tags: log-engine, memory-engine
-- log-engine, memory-engine: the defect is in these engines' reads; with MergeTree in their place the test checks nothing.

-- An EPHEMERAL column named like a subcolumn of a stored column is not stored, so reading it is rejected
-- as for any other EPHEMERAL column, and the stored columns and their subcolumns stay readable.

DROP TABLE IF EXISTS t_eph_log;
DROP TABLE IF EXISTS t_eph_tinylog;
DROP TABLE IF EXISTS t_eph_stripelog;
DROP TABLE IF EXISTS t_eph_memory;
DROP TABLE IF EXISTS t_eph_buffer;
DROP TABLE IF EXISTS t_eph_buffer_dst;
DROP TABLE IF EXISTS t_eph_join;
DROP TABLE IF EXISTS t_eph_mv;
DROP TABLE IF EXISTS t_eph_mv_src;
DROP TABLE IF EXISTS t_eph_mv_dst;

CREATE TABLE t_eph_log (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = Log;
INSERT INTO t_eph_log (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys` FROM t_eph_log ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, `a.size0` FROM t_eph_log ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, `t.x` FROM t_eph_log ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, m, a, t, y, mapKeys(m), length(a), tupleElement(t, 'x') FROM t_eph_log ORDER BY id;
SELECT count() FROM t_eph_log SETTINGS optimize_trivial_count_query = 0;

CREATE TABLE t_eph_tinylog (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = TinyLog;
INSERT INTO t_eph_tinylog (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys` FROM t_eph_tinylog ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, m, a, t, y, mapKeys(m), length(a), tupleElement(t, 'x') FROM t_eph_tinylog ORDER BY id;

CREATE TABLE t_eph_stripelog (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = StripeLog;
INSERT INTO t_eph_stripelog (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys` FROM t_eph_stripelog ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, m, a, t, y, mapKeys(m), length(a), tupleElement(t, 'x') FROM t_eph_stripelog ORDER BY id;

CREATE TABLE t_eph_memory (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = Memory;
INSERT INTO t_eph_memory (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys` FROM t_eph_memory ORDER BY id; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, `a.size0` FROM t_eph_memory ORDER BY id LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT id, m, a, t, y, mapKeys(m), length(a), tupleElement(t, 'x') FROM t_eph_memory ORDER BY id;
SELECT count() FROM t_eph_memory SETTINGS optimize_trivial_count_query = 0;

CREATE TABLE t_eph_buffer_dst (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL) ENGINE = Memory;
CREATE TABLE t_eph_buffer (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL)
    ENGINE = Buffer(currentDatabase(), t_eph_buffer_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_eph_buffer (id, a) VALUES (1, [1, 2, 3]);
SELECT id, `a.size0` FROM t_eph_buffer; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

CREATE TABLE t_eph_join (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL) ENGINE = Join(ANY, LEFT, id);
INSERT INTO t_eph_join (id, a) VALUES (1, [1, 2, 3]);
SELECT id, `a.size0` FROM t_eph_join; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

CREATE TABLE t_eph_mv_src (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL) ENGINE = Null;
CREATE TABLE t_eph_mv_dst (id UInt64, s UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW t_eph_mv TO t_eph_mv_dst AS SELECT id, `a.size0` AS s FROM t_eph_mv_src;
INSERT INTO t_eph_mv_src (id, a, `a.size0`) VALUES (1, [1, 2, 3], 7); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT count() FROM t_eph_mv_dst;

DROP TABLE t_eph_log;
DROP TABLE t_eph_tinylog;
DROP TABLE t_eph_stripelog;
DROP TABLE t_eph_memory;
DROP TABLE t_eph_buffer;
DROP TABLE t_eph_buffer_dst;
DROP TABLE t_eph_join;
DROP TABLE t_eph_mv;
DROP TABLE t_eph_mv_src;
DROP TABLE t_eph_mv_dst;
