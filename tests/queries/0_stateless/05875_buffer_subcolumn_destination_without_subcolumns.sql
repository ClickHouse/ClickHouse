-- Reading subcolumns through a `Buffer` table whose destination can not read subcolumns (`StripeLog`, `Join`).
-- https://github.com/ClickHouse/ClickHouse/issues/134307

-- `StorageBuffer` warns about every column whose type differs from the destination - do not send these warnings to the client.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_134307_merge;
DROP TABLE IF EXISTS t_134307_buf_cast;
DROP TABLE IF EXISTS t_134307_buf;
DROP TABLE IF EXISTS t_134307_dst;
DROP TABLE IF EXISTS t_134307_join_buf;
DROP TABLE IF EXISTS t_134307_join;

CREATE TABLE t_134307_dst (k UInt8, x Nullable(UInt8), a Array(UInt8), t Tuple(a UInt8, b String)) ENGINE = StripeLog;
INSERT INTO t_134307_dst VALUES (1, 1, [1], (1, 'a')), (2, NULL, [1, 2], (2, 'b')), (3, 3, [], (3, 'c'));

CREATE TABLE t_134307_buf AS t_134307_dst ENGINE = Buffer(currentDatabase(), t_134307_dst, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_134307_buf VALUES (4, NULL, [4, 4, 4], (4, 'd'));

SELECT 'StripeLog';
SELECT x.null FROM t_134307_buf ORDER BY x.null;
SELECT k, x.null, a.size0, t.a, t.b FROM t_134307_buf ORDER BY k;
SELECT k FROM t_134307_buf WHERE x.null ORDER BY k;
SELECT k FROM t_134307_buf WHERE a.size0 > 0 AND t.a > 1 ORDER BY k;
SELECT sum(a.size0) FROM t_134307_buf;
SELECT k, x.null, a.size0 FROM t_134307_buf ORDER BY k SETTINGS enable_analyzer = 0;

SELECT 'Different type';
CREATE TABLE t_134307_buf_cast (k UInt8, x Nullable(UInt16), a Array(UInt16)) ENGINE = Buffer(currentDatabase(), t_134307_dst, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
SELECT k, x.null, a.size0 FROM t_134307_buf_cast ORDER BY k;

SELECT 'Merge';
CREATE TABLE t_134307_merge AS t_134307_dst ENGINE = Merge(currentDatabase(), '^t_134307_buf$');
SELECT k, x.null, a.size0, t.a FROM t_134307_merge ORDER BY k;

SELECT 'Join';
CREATE TABLE t_134307_join (k UInt8, x Nullable(UInt8), a Array(UInt8)) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_134307_join VALUES (1, 1, [1]), (2, NULL, [1, 2]);
CREATE TABLE t_134307_join_buf AS t_134307_join ENGINE = Buffer(currentDatabase(), t_134307_join, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
SELECT k, x.null, a.size0 FROM t_134307_join_buf ORDER BY k;

DROP TABLE t_134307_merge;
DROP TABLE t_134307_buf_cast;
DROP TABLE t_134307_buf;
DROP TABLE t_134307_dst;
DROP TABLE t_134307_join_buf;
DROP TABLE t_134307_join;
