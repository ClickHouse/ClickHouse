-- Regression test: a tuple-element `DEFAULT NULL` must not wrap `SimpleAggregateFunction` in
-- `Nullable` when its storage type is already nullable. It used to produce the invalid type
-- `Nullable(SimpleAggregateFunction(any, Nullable(UInt64)))`, rejected with `ILLEGAL_TYPE_OF_ARGUMENT`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_saf_nullable;

CREATE TABLE t_tuple_default_saf_nullable
(
    id UInt8,
    c Tuple(a SimpleAggregateFunction(any, Nullable(UInt64)) DEFAULT NULL)
)
ENGINE = Memory;

ALTER TABLE t_tuple_default_saf_nullable ADD COLUMN d Tuple(a SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))) DEFAULT NULL);

SELECT name, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_default_saf_nullable' AND name != 'id'
ORDER BY name;

INSERT INTO t_tuple_default_saf_nullable (id) VALUES (1);
SELECT c, d FROM t_tuple_default_saf_nullable;

DROP TABLE t_tuple_default_saf_nullable;
