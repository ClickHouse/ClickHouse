-- An EPHEMERAL column is insert-only, so in a SELECT its name means the subcolumn of the same name, if any
-- (`a.size0` next to `a Array`), on every engine. INSERT and DEFAULT expressions still see the column.

DROP TABLE IF EXISTS t_eph_mt;
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

CREATE TABLE t_eph_mt (id UInt64, e UInt64 EPHEMERAL, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_eph_mt (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, e FROM t_eph_mt ORDER BY id; -- { serverError UNKNOWN_IDENTIFIER }
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_mt ORDER BY id;
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_mt ORDER BY id SETTINGS allow_experimental_analyzer = 0;
SELECT count() FROM t_eph_mt WHERE `a.size0` = 2 SETTINGS optimize_trivial_count_query = 0;

CREATE TABLE t_eph_log (id UInt64, e UInt64 EPHEMERAL, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = Log;
INSERT INTO t_eph_log (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, e FROM t_eph_log ORDER BY id; -- { serverError UNKNOWN_IDENTIFIER }
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_log ORDER BY id;
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_log ORDER BY id SETTINGS allow_experimental_analyzer = 0;

CREATE TABLE t_eph_tinylog (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = TinyLog;
INSERT INTO t_eph_tinylog (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_tinylog ORDER BY id;

CREATE TABLE t_eph_stripelog (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = StripeLog;
INSERT INTO t_eph_stripelog (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_stripelog ORDER BY id;

CREATE TABLE t_eph_memory (id UInt64, m Map(String, UInt64), `m.keys` UInt128 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL,
    t Tuple(x UInt64), `t.x` String EPHEMERAL, y UInt64 DEFAULT `a.size0` * 10) ENGINE = Memory;
INSERT INTO t_eph_memory (id, m, a, t, `a.size0`) VALUES (1, map('k', 1), [1], tuple(1), 7), (2, map('l', 2, 'n', 3), [2, 3], tuple(2), 8);
SELECT id, `m.keys`, `a.size0`, `t.x`, y FROM t_eph_memory ORDER BY id;
SELECT id, `a.size0` FROM t_eph_memory ORDER BY id LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 0;

CREATE TABLE t_eph_buffer_dst (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL) ENGINE = Memory;
CREATE TABLE t_eph_buffer (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL)
    ENGINE = Buffer(currentDatabase(), t_eph_buffer_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_eph_buffer (id, a) VALUES (1, [1, 2, 3]);
SELECT id, `a.size0` FROM t_eph_buffer;

CREATE TABLE t_eph_join (id UInt64, a Array(UInt64), `a.size0` UInt64 EPHEMERAL) ENGINE = Join(ANY, LEFT, id);
INSERT INTO t_eph_join (id, a) VALUES (1, [1, 2, 3]);
SELECT id, `a.size0` FROM t_eph_join;

CREATE TABLE t_eph_mv_src (id UInt64, e UInt64 EPHEMERAL, a Array(UInt64), `a.size0` UInt64 EPHEMERAL) ENGINE = Null;
CREATE TABLE t_eph_mv_dst (id UInt64, s UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW t_eph_mv TO t_eph_mv_dst AS SELECT id, e AS s FROM t_eph_mv_src; -- { serverError UNKNOWN_IDENTIFIER }
CREATE MATERIALIZED VIEW t_eph_mv TO t_eph_mv_dst AS SELECT id, `a.size0` AS s FROM t_eph_mv_src;
INSERT INTO t_eph_mv_src (id, a, `a.size0`) VALUES (1, [1, 2, 3], 7);
SELECT * FROM t_eph_mv_dst;

DROP TABLE t_eph_mt;
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
