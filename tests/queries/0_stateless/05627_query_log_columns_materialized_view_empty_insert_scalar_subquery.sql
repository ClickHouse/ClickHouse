-- An insert that moves no rows does not analyze or run the SELECT of a dependent view, so a scalar subquery in it is
-- not executed, wherever it is: in the projection, in a table function argument, or in a parameterized view argument.
-- The analyzer executes the last two even in analyze-only mode. The log lists such a view in `views` and its target
-- by name only.
SET async_insert = 0;

DROP TABLE IF EXISTS mv_projection;
DROP TABLE IF EXISTS mv_table_function;
DROP TABLE IF EXISTS mv_parameterized_view;
DROP TABLE IF EXISTS pv;
DROP TABLE IF EXISTS src_projection;
DROP TABLE IF EXISTS src_table_function;
DROP TABLE IF EXISTS src_parameterized_view;
DROP TABLE IF EXISTS lookup;
DROP TABLE IF EXISTS dst;

CREATE TABLE lookup (b UInt64) ENGINE = MergeTree ORDER BY b;
CREATE TABLE dst (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE src_projection (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE src_table_function (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE src_parameterized_view (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW pv AS SELECT number AS c FROM numbers({n:UInt64});

CREATE MATERIALIZED VIEW mv_projection TO dst AS
    SELECT a, (SELECT b FROM lookup) AS c FROM src_projection;
CREATE MATERIALIZED VIEW mv_table_function TO dst AS
    SELECT a, n.number AS c FROM src_table_function CROSS JOIN numbers(assumeNotNull((SELECT b FROM lookup))) AS n;
CREATE MATERIALIZED VIEW mv_parameterized_view TO dst AS
    SELECT a, p.c FROM src_parameterized_view CROSS JOIN pv(n = assumeNotNull((SELECT b FROM lookup))) AS p;

-- Two rows, so the scalar subquery fails whenever it runs. Inserted after the views are created, because
-- CREATE MATERIALIZED VIEW analyzes the SELECT.
INSERT INTO lookup VALUES (1), (2);

INSERT INTO src_projection SETTINGS log_comment = '05627_empty_projection' SELECT 1 WHERE 0;
INSERT INTO src_table_function SETTINGS log_comment = '05627_empty_table_function' SELECT 1 WHERE 0;
INSERT INTO src_parameterized_view SETTINGS log_comment = '05627_empty_parameterized_view' SELECT 1 WHERE 0;

-- With a row, the SELECT of the view runs and the scalar subquery fails
INSERT INTO src_projection VALUES (1); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
INSERT INTO src_table_function VALUES (1); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
INSERT INTO src_parameterized_view VALUES (1); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

SELECT 'dst', count() FROM dst;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, type, tables, views, columns
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE '05627\_empty\_%'
  AND type = 'QueryFinish'
ORDER BY log_comment
FORMAT Vertical;

DROP TABLE mv_projection;
DROP TABLE mv_table_function;
DROP TABLE mv_parameterized_view;
DROP TABLE pv;
DROP TABLE src_projection;
DROP TABLE src_table_function;
DROP TABLE src_parameterized_view;
DROP TABLE lookup;
DROP TABLE dst;
