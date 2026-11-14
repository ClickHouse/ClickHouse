-- `force_primary_key_reverse_order` must produce table metadata that can be parsed back when the sorting key
-- is a grouped tuple, such as `ORDER BY ((a, b))`, `ORDER BY (tuple(a, b))` or `PRIMARY KEY ((a, b))`.
-- The rewritten tuple used to keep its parentheses and was stored as `ORDER BY (tuple(a DESC, b DESC))`,
-- which cannot be parsed, so `SHOW CREATE TABLE` and `ATTACH TABLE` failed.

SET force_primary_key_reverse_order = 1;

DROP TABLE IF EXISTS t_grouped_tuple;
DROP TABLE IF EXISTS t_grouped_function;
DROP TABLE IF EXISTS t_grouped_pk;

CREATE TABLE t_grouped_tuple (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY ((a, b));
CREATE TABLE t_grouped_function (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (tuple(a, b));
CREATE TABLE t_grouped_pk (a UInt64, b UInt64) ENGINE = MergeTree PRIMARY KEY ((a, b));

INSERT INTO t_grouped_tuple VALUES (1, 2), (3, 4);
INSERT INTO t_grouped_function VALUES (1, 2), (3, 4);
INSERT INTO t_grouped_pk VALUES (1, 2), (3, 4);

SELECT name, extract(create_table_query, 'ORDER BY (.*) SETTINGS') FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_grouped_%' ORDER BY name;

DETACH TABLE t_grouped_tuple;
ATTACH TABLE t_grouped_tuple;
DETACH TABLE t_grouped_function;
ATTACH TABLE t_grouped_function;
DETACH TABLE t_grouped_pk;
ATTACH TABLE t_grouped_pk;

SELECT name, extract(create_table_query, 'ORDER BY (.*) SETTINGS') FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_grouped_%' ORDER BY name;

SELECT a, b FROM t_grouped_tuple ORDER BY a, b;
SELECT a, b FROM t_grouped_function ORDER BY a, b;
SELECT a, b FROM t_grouped_pk ORDER BY a, b;

DROP TABLE t_grouped_tuple;
DROP TABLE t_grouped_function;
DROP TABLE t_grouped_pk;
