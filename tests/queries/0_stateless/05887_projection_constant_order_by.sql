-- a projection with a constant `ORDER BY` gives the error of the key check

DROP TABLE IF EXISTS t_projection_constant_order_by;

CREATE TABLE t_projection_constant_order_by (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT a ORDER BY 78); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT a ORDER BY (1, 2)); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT a ORDER BY 1 + 1); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT a ORDER BY tuple()); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT a, b WHERE b = 1 ORDER BY (1, 2)); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p INDEX 78 TYPE basic(); -- { serverError ILLEGAL_COLUMN }

-- the projection reads no column of the table
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT 1 AS x ORDER BY x); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p (SELECT 1); -- { serverError BAD_ARGUMENTS }

-- the expression of the index has no columns
ALTER TABLE t_projection_constant_order_by ADD PROJECTION p_no_columns INDEX * EXCEPT '.*' TYPE basic();

SELECT name, sorting_key FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_constant_order_by'
ORDER BY name;

DROP TABLE t_projection_constant_order_by;
