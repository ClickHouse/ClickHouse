-- the blending engine check must follow a table in the right operand of `IN` in the query of a view

CREATE TABLE src (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k;

CREATE VIEW v_in AS SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE (k, v) IN src;
CREATE VIEW v_not_in AS SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE (k, v) NOT IN src;
CREATE VIEW v_global_in AS SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE (k, v) GLOBAL IN src;
CREATE VIEW v_global_not_in AS SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE (k, v) GLOBAL NOT IN src;
CREATE VIEW v_null_in AS SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE nullIn((k, v), src);
CREATE VIEW v_subquery AS SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE (k, v) IN (SELECT * FROM src);

CREATE ROW POLICY p ON v_in USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_not_in USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_global_in USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_global_not_in USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_null_in USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY p ON v_subquery USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }

-- the query of a view read back from the metadata
DETACH TABLE v_in;
ATTACH TABLE v_in;
CREATE ROW POLICY p ON v_in USING v % 2 = 1 TO ALL; -- { serverError BAD_ARGUMENTS }

-- a common table expression and an alias of an expression with the name of the table are not the table
CREATE VIEW v_cte AS WITH src AS (SELECT toUInt64(1), toUInt64(1)) SELECT toUInt64(1) AS k, toUInt64(1) AS v WHERE (k, v) IN src;
CREATE VIEW v_alias AS SELECT toUInt64(1) AS v, [toUInt64(1)] AS src WHERE v IN src;
CREATE ROW POLICY p ON v_cte USING v % 2 = 1 TO ALL;
CREATE ROW POLICY p ON v_alias USING v % 2 = 1 TO ALL;
SELECT * FROM v_cte;
SELECT * FROM v_alias;

DROP ROW POLICY p ON v_cte;
DROP ROW POLICY p ON v_alias;
