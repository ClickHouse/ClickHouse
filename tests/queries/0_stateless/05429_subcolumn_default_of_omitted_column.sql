DROP TABLE IF EXISTS t_omit;
CREATE TABLE t_omit
(
    line String,
    j Tuple(a Int64, b Int64),
    arr Array(Int64),
    x Int64 MATERIALIZED j.a,
    n UInt64 MATERIALIZED arr.size0,
    s Int64 DEFAULT j.b
) ENGINE = MergeTree ORDER BY tuple();

-- The parents are omitted: they take their type defaults, and the subcolumns are read from those.
INSERT INTO t_omit (line) VALUES ('omitted');
INSERT INTO t_omit (line) SELECT 'omitted by select';
INSERT INTO t_omit (line, j, arr) VALUES ('present', (1, 2), [1, 2, 3]);
SELECT line, x, n, s FROM t_omit ORDER BY line;

-- The input-format path for omitted fields must keep working (the blank line ends the inline data).
SET input_format_defaults_for_omitted_fields = 1;
INSERT INTO t_omit FORMAT JSONEachRow {"line":"json row"}

SELECT line, x, n, s FROM t_omit WHERE line = 'json row';

DROP TABLE t_omit;
