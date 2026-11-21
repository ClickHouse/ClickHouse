SET enable_analyzer = 1;

-- The pending mutations applied with `apply_mutations_on_fly` are not applied to a subcolumn read directly,
-- so the subcolumns of the columns they change must not be pushed into the subquery.

DROP TABLE IF EXISTS t_subcolumn_pushdown_mutations;

CREATE TABLE t_subcolumn_pushdown_mutations (tup Tuple(a UInt64, b String), arr Array(UInt64), n Nullable(UInt8), m Tuple(c UInt64))
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES t_subcolumn_pushdown_mutations;

INSERT INTO t_subcolumn_pushdown_mutations VALUES ((1, 'x'), [1, 2], 1, tuple(10)), ((2, 'y'), [3], 2, tuple(20));
ALTER TABLE t_subcolumn_pushdown_mutations UPDATE tup = tuple(42, 'z'), arr = [1, 2, 3], n = NULL WHERE 1 SETTINGS mutations_sync = 0;

SELECT sum(tup.a), sum(arr.size0), sum(n.null), sum(m.c) FROM (SELECT tup, arr, n, m FROM t_subcolumn_pushdown_mutations)
SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 0;

SELECT sum(tup.a), sum(arr.size0), sum(n.null), sum(m.c) FROM (SELECT tup, arr, n, m FROM t_subcolumn_pushdown_mutations)
SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 1;

-- The column changed by the mutation is read whole, the column not changed by it is still read as a subcolumn.
SELECT count() > 0 FROM (
    EXPLAIN header = 1 SELECT sum(tup.a), sum(m.c) FROM (SELECT tup, m FROM t_subcolumn_pushdown_mutations)
    SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 1
) WHERE explain LIKE '%tup Tuple%';
SELECT count() > 0 FROM (
    EXPLAIN header = 1 SELECT sum(tup.a), sum(m.c) FROM (SELECT tup, m FROM t_subcolumn_pushdown_mutations)
    SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 1
) WHERE explain LIKE '%m.c UInt64%';

-- Without `apply_mutations_on_fly` the old values are read both ways.
SELECT sum(tup.a), sum(arr.size0), sum(n.null) FROM (SELECT tup, arr, n FROM t_subcolumn_pushdown_mutations)
SETTINGS apply_mutations_on_fly = 0, optimize_push_subcolumns_into_subqueries = 1;

-- A `MATERIALIZED` column is recalculated on the fly when a column it depends on (directly, or through an `ALIAS`
-- and another `MATERIALIZED` column) is updated, so its subcolumns must not be pushed into the subquery either.

DROP TABLE IF EXISTS t_subcolumn_pushdown_mutations_materialized;

CREATE TABLE t_subcolumn_pushdown_mutations_materialized
(
    x UInt64,
    m Tuple(a UInt64, b String) MATERIALIZED tuple(x, 'z'),
    al UInt64 ALIAS x + 1,
    m1 UInt64 MATERIALIZED al * 2,
    m2 Tuple(a UInt64) MATERIALIZED tuple(m1)
)
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES t_subcolumn_pushdown_mutations_materialized;

INSERT INTO t_subcolumn_pushdown_mutations_materialized (x) VALUES (1), (2);
ALTER TABLE t_subcolumn_pushdown_mutations_materialized UPDATE x = 42 WHERE 1 SETTINGS mutations_sync = 0;

SELECT sum(m.a), sum(m2.a) FROM (SELECT m, m2 FROM t_subcolumn_pushdown_mutations_materialized)
SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 0;

SELECT sum(m.a), sum(m2.a) FROM (SELECT m, m2 FROM t_subcolumn_pushdown_mutations_materialized)
SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 1;

SELECT sum(m.a), sum(m2.a) FROM (SELECT m, m2 FROM (SELECT m, m2 FROM t_subcolumn_pushdown_mutations_materialized))
SETTINGS apply_mutations_on_fly = 1, optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE t_subcolumn_pushdown_mutations_materialized;

-- The table is read with the settings of the subquery, which can enable `apply_mutations_on_fly` even if the outer
-- query does not.

DROP TABLE IF EXISTS t_subcolumn_pushdown_mutations_merge;

CREATE TABLE t_subcolumn_pushdown_mutations_merge (tup Tuple(a UInt64, b String), arr Array(UInt64), n Nullable(UInt8), m Tuple(c UInt64))
ENGINE = Merge(currentDatabase(), '^t_subcolumn_pushdown_mutations$');

SELECT sum(tup.a) FROM (SELECT tup FROM t_subcolumn_pushdown_mutations_merge SETTINGS apply_mutations_on_fly = 1)
SETTINGS apply_mutations_on_fly = 0, optimize_push_subcolumns_into_subqueries = 0;

SELECT sum(tup.a) FROM (SELECT tup FROM t_subcolumn_pushdown_mutations_merge SETTINGS apply_mutations_on_fly = 1)
SETTINGS apply_mutations_on_fly = 0, optimize_push_subcolumns_into_subqueries = 1;

SELECT sum(tup.a) FROM (SELECT tup FROM t_subcolumn_pushdown_mutations SETTINGS apply_mutations_on_fly = 1)
SETTINGS apply_mutations_on_fly = 0, optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE t_subcolumn_pushdown_mutations_merge;

DROP TABLE t_subcolumn_pushdown_mutations;
