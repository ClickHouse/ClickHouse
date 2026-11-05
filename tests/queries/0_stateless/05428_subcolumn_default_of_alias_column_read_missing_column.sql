SET enable_json_type = 1;

DROP TABLE IF EXISTS t_read;
DROP TABLE IF EXISTS t_read_eph;
DROP TABLE IF EXISTS t_read_plain;

-- `flag` is smaller than `line`: `line` is read only if the defaults really ask for it.
CREATE TABLE t_read
(
    flag UInt8,
    line String,
    j Tuple(a Int64, b Int64) ALIAS JSONExtract(line, 'Tuple(a Int64, b Int64)'),
    js JSON ALIAS line::JSON
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_read (flag, line) VALUES (1, '{"a":1,"b":2}');
INSERT INTO t_read (flag, line) VALUES (2, '{"a":3,"b":4}');

-- The parts lack x and y: they are filled from the defaults while reading.
ALTER TABLE t_read ADD COLUMN x Int64 DEFAULT j.a, ADD COLUMN y Int64 DEFAULT js.b::Int64;
SELECT x, y FROM t_read ORDER BY flag;
SELECT x, y, line FROM t_read ORDER BY flag;

-- The mutation path fills them the same way.
ALTER TABLE t_read MATERIALIZE COLUMN x SETTINGS mutations_sync = 2;
SELECT x, y FROM t_read ORDER BY flag;

-- The merge fills them the same way.
OPTIMIZE TABLE t_read FINAL;
SELECT x, y FROM t_read ORDER BY flag;

-- The parent is EPHEMERAL: its expression is evaluated from `line` while reading.
CREATE TABLE t_read_eph
(
    flag UInt8,
    line String,
    j Tuple(a Int64, b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Int64, b Int64)')
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_read_eph (flag, line) VALUES (1, '{"a":1,"b":2}');
INSERT INTO t_read_eph (flag, line) VALUES (2, '{"a":3,"b":4}');
ALTER TABLE t_read_eph ADD COLUMN y Int64 MATERIALIZED j.b;
SELECT y FROM t_read_eph ORDER BY flag;

-- The parent is a stored column without a default, added together with the dependent column:
-- its type default must carry the declared type for `j.a` to resolve.
CREATE TABLE t_read_plain (flag UInt8, line String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_read_plain (flag, line) VALUES (1, 'x');
ALTER TABLE t_read_plain ADD COLUMN j Tuple(a Int64, b Int64), ADD COLUMN x Int64 DEFAULT j.a + 5;
SELECT x FROM t_read_plain;

DROP TABLE t_read;
DROP TABLE t_read_eph;
DROP TABLE t_read_plain;
