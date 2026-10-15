-- The lazy metadata-only `ALTER` guard for `MATERIALIZED` columns expands matchers in stored
-- expressions with schema-only semantics: `*` never includes `ALIAS` columns there, regardless
-- of `asterisk_include_alias_columns` in the session.

SET enable_analyzer = 1;
SET asterisk_include_alias_columns = 1;
SET allow_metadata_only_named_tuple_alter = 1;

DROP TABLE IF EXISTS t_lazy_matcher;

CREATE TABLE t_lazy_matcher
(
    a Tuple(x UInt8),
    b UInt8,
    al Tuple(x UInt8) ALIAS a,
    m String MATERIALIZED toJSONString(tuple(* EXCEPT (m, a)))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_lazy_matcher (a, b) VALUES (tuple(1), 2);

-- `m` reads only `b`, so the lazy conversion of `a` does not leave it stale.
ALTER TABLE t_lazy_matcher MODIFY COLUMN a Tuple(x UInt8, y UInt8);

SELECT a, b, m FROM t_lazy_matcher;

DROP TABLE IF EXISTS t_lazy_matcher_dep;

-- A `MATERIALIZED` column that does read `a` through the matcher is still protected.
CREATE TABLE t_lazy_matcher_dep
(
    a Tuple(x UInt8),
    b UInt8,
    m String MATERIALIZED toJSONString(tuple(* EXCEPT m))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_lazy_matcher_dep (a, b) VALUES (tuple(1), 2);

ALTER TABLE t_lazy_matcher_dep MODIFY COLUMN a Tuple(x UInt8, y UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_lazy_matcher;
DROP TABLE t_lazy_matcher_dep;
