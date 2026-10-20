SET enable_json_type = 1;

DROP TABLE IF EXISTS t_read;
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

-- The merge fills them the same way.
OPTIMIZE TABLE t_read FINAL;
SELECT x, y FROM t_read ORDER BY flag;

DROP TABLE t_read;
