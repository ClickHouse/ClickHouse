-- A `CHECK` constraint that reads a subcolumn takes it from the column that the table gives the name: next to
-- a `JSON` column `a` and a `Tuple` column `a.x`, the subcolumn `a.x.y` is the element `y` of `a.x`, not the
-- path `x.y` of `a`.

DROP TABLE IF EXISTS t_shadow_constraint;
CREATE TABLE t_shadow_constraint
(
    id UInt64,
    a JSON,
    `a.x` Tuple(y Dynamic),
    CONSTRAINT c CHECK toUInt64(`a.x.y`) = 44
)
ENGINE = MergeTree ORDER BY id;

-- The tuple element satisfies the constraint, the path of the `JSON` column does not.
INSERT INTO t_shadow_constraint VALUES (1, '{"x" : {"y" : 11}}', tuple(44::UInt64));
INSERT INTO t_shadow_constraint SELECT 2, '{"x" : {"y" : 11}}', tuple(44::UInt64);

-- The path of the `JSON` column satisfies the constraint, the tuple element does not.
INSERT INTO t_shadow_constraint VALUES (3, '{"x" : {"y" : 44}}', tuple(11::UInt64)); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_shadow_constraint SELECT 4, '{"x" : {"y" : 44}}', tuple(11::UInt64); -- { serverError VIOLATED_CONSTRAINT }

SELECT id, `a.x.y` FROM t_shadow_constraint ORDER BY id;

DROP TABLE t_shadow_constraint;
