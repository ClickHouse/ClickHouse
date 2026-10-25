-- Regression test: with `enable_global_with_statement` disabled, the recursive member of a recursive common table expression
-- does not see it, so the bare name there is a table. A view reading such a table is its dependent, and the table cannot be
-- dropped while `check_referential_table_dependencies` is enabled.

SET enable_analyzer = 1;

CREATE TABLE r (n UInt64) ENGINE = Memory;
INSERT INTO r VALUES (2);

CREATE VIEW v_setting AS
    WITH RECURSIVE r AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3)
    SELECT * FROM r SETTINGS enable_global_with_statement = 0;

-- The setting of an enclosing select query applies to the nested ones.
CREATE VIEW v_enclosing_setting AS
    SELECT * FROM (WITH RECURSIVE r AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r)
    SETTINGS enable_global_with_statement = 0;

-- A trailing `SETTINGS` clause of the body applies to every member.
CREATE VIEW v_body_setting AS
    WITH RECURSIVE r AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3 SETTINGS enable_global_with_statement = 0)
    SELECT * FROM r;

-- With the setting enabled, `r` in the recursive member is the common table expression.
CREATE VIEW v_cte AS
    WITH RECURSIVE r AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3)
    SELECT * FROM r;

SELECT 'v_setting', * FROM v_setting ORDER BY n;
SELECT 'v_enclosing_setting', * FROM v_enclosing_setting ORDER BY n;
SELECT 'v_body_setting', * FROM v_body_setting ORDER BY n;
SELECT 'v_cte', * FROM v_cte ORDER BY n;

SELECT name, dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'r';

DROP TABLE r SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }

DROP VIEW v_setting;
DROP VIEW v_enclosing_setting;
DROP VIEW v_body_setting;
DROP TABLE r SETTINGS check_referential_table_dependencies = 1;
DROP VIEW v_cte;
