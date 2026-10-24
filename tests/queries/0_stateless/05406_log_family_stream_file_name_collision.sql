-- Tags: log-engine
-- Log and TinyLog store every stream in a file named after it, so two streams with one file name cannot be stored.

DROP TABLE IF EXISTS t_collision;

-- Two streams of one column: the elements of `a.values` and the values of the Map `a`.
CREATE TABLE t_collision (c Tuple(`a.values` Array(LowCardinality(String)), a Map(String, String))) ENGINE = TinyLog; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Tuple(`a.values` Array(LowCardinality(String)), a Map(String, String))) ENGINE = Log; -- { serverError BAD_ARGUMENTS }

-- Streams of two columns.
CREATE TABLE t_collision (`c.a` UInt64, c Tuple(a UInt64)) ENGINE = TinyLog; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (`c.a` UInt64, c Tuple(a UInt64)) ENGINE = Log; -- { serverError BAD_ARGUMENTS }

-- The array sizes shared by a Nested group are one stream.
CREATE TABLE t_collision (n Nested(a UInt64, b String), `m.a` Array(UInt64), `m.b` Array(String)) ENGINE = TinyLog;
INSERT INTO t_collision VALUES ([1, 2], ['x', 'y'], [3], ['z']);
SELECT * FROM t_collision;
DROP TABLE t_collision;

-- The non-flattened Nested `n.a` next to the flattened group `n` shares the array sizes with it, and its element `x` with `n.a.x`.
SET flatten_nested = 0;
CREATE TABLE t_collision (`n.a` Nested(x UInt64), `n.b` Array(UInt64), `n.a.x` UInt64) ENGINE = TinyLog; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (`n.a` Nested(x UInt64), `n.b` Array(UInt64), `n.a.x` UInt64) ENGINE = Log; -- { serverError BAD_ARGUMENTS }
