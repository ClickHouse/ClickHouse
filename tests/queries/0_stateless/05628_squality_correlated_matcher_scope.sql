SET prefer_outer_table_name_for_correlated_identifier = 1;
SET analyzer_compatibility_prefer_alias_over_subcolumn = 1;

CREATE TABLE squality_matcher_scope_outer (x UInt64) ENGINE = Memory;
CREATE TABLE squality_matcher_scope_inner (x UInt64, t Tuple(z UInt64)) ENGINE = Memory;
INSERT INTO squality_matcher_scope_outer VALUES (1), (2), (3);
INSERT INTO squality_matcher_scope_inner VALUES (4, (100)), (5, (200));

-- The outer table alias takes precedence over a tuple column in the inner query only
-- when the tuple column belongs to the same scope as the table alias.
SELECT x, (
    SELECT tuple(t.*) FROM squality_matcher_scope_outer AS i WHERE i.x = 3 LIMIT 1
)
FROM squality_matcher_scope_inner AS t ORDER BY x;

-- A nearer tuple column still wins over the parent table alias.
SELECT x, (
    SELECT tuple(t.*) FROM squality_matcher_scope_inner AS i ORDER BY i.x LIMIT 1
)
FROM squality_matcher_scope_inner AS t ORDER BY x;

DROP TABLE squality_matcher_scope_outer;
DROP TABLE squality_matcher_scope_inner;

SET group_by_use_nulls = 1;
CREATE TABLE squality_matcher_scope_tuple (v Tuple(x UInt64)) ENGINE = Memory;
INSERT INTO squality_matcher_scope_tuple VALUES ((1)), ((2)), ((3));

-- The inner grouping key must not replace a generated subcolumn of the outer table.
SELECT t.v.x, (
    SELECT t.v.* FROM squality_matcher_scope_tuple AS i WHERE i.v.x = 3
    GROUP BY ROLLUP(getSubcolumn(i.v, 'x'))
    HAVING GROUPING(getSubcolumn(i.v, 'x')) = 0 LIMIT 1
)
FROM squality_matcher_scope_tuple AS t ORDER BY t.v.x; -- { serverError NOT_AN_AGGREGATE }

DROP TABLE squality_matcher_scope_tuple;
