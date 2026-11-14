-- A `REPLACE` target that the matcher never produces, because `*` skips `MATERIALIZED` and `ALIAS` columns,
-- cannot turn a `GROUP BY` key into a positional argument, so it must not force an unrelated `APPLY`
-- to be validated against the non-Nullable source columns.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_materialized;
DROP TABLE IF EXISTS t_alias;

CREATE TABLE t_materialized (c UInt64, m UInt64 MATERIALIZED c + 1) ENGINE = Memory;
INSERT INTO t_materialized (c) SELECT number FROM numbers(3);

CREATE TABLE t_alias (c UInt64, m UInt64 ALIAS c + 1) ENGINE = Memory;
INSERT INTO t_alias (c) SELECT number FROM numbers(3);

SELECT 'materialized, unqualified';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (1 AS m)
FROM t_materialized AS t
GROUP BY m, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'materialized, qualified';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), t.* REPLACE (1 AS m)
FROM t_materialized AS t
GROUP BY m, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'alias, unqualified';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (1 AS m)
FROM t_alias AS t
GROUP BY m, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- With `asterisk_include_materialized_columns` the matcher produces `m`, and its replacement is a positional argument.
SELECT 'materialized, included';
SELECT t.c, * REPLACE (1 AS m)
FROM t_materialized AS t
GROUP BY m, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1, asterisk_include_materialized_columns = 1;

DROP TABLE t_materialized;
DROP TABLE t_alias;
