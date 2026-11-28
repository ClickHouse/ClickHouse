-- A subcolumn of a `JSON` column whose type hints were changed lazily is not pushed into a subquery while a part
-- still stores the column under the old type: reading the whole column converts it, reading the subcolumn does not.

DROP TABLE IF EXISTS t_lazy_hints;
CREATE TABLE t_lazy_hints (j JSON(b UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_lazy_hints VALUES ('{"b":7}'), ('{"b":8}');

SELECT 'before alter';
SELECT sum(j.b) FROM (SELECT j FROM t_lazy_hints) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT sum(j.b) FROM (SELECT j FROM t_lazy_hints) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

ALTER TABLE t_lazy_hints MODIFY COLUMN j JSON() SETTINGS enable_json_lazy_type_hints = 1;

SELECT 'after lazy alter';
SELECT sum(j.b.:Int64) FROM (SELECT j FROM t_lazy_hints) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT sum(j.b.:Int64) FROM (SELECT j FROM t_lazy_hints) SETTINGS optimize_push_subcolumns_into_subqueries = 1;
WITH c AS (SELECT j FROM t_lazy_hints) SELECT sum(j.b.:Int64) FROM c SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'after merge';
OPTIMIZE TABLE t_lazy_hints FINAL;
SELECT sum(j.b.:Int64) FROM (SELECT j FROM t_lazy_hints) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT sum(j.b.:Int64) FROM (SELECT j FROM t_lazy_hints) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE t_lazy_hints;
