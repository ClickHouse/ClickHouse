-- Standalone expressions (keys, indices, constraints) have no aggregation or window step, so aggregate
-- and window functions in them must be rejected before the metadata is stored. An aggregate inside
-- an `IN` set subquery is fine.

DROP TABLE IF EXISTS standalone_expression_aggregates;

CREATE TABLE standalone_expression_aggregates
(
    x UInt64,
    INDEX i (sum(x) + 1) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_AGGREGATION }

CREATE TABLE standalone_expression_aggregates
(
    x UInt64,
    INDEX i (sum(x) OVER ()) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_AGGREGATION }

CREATE TABLE standalone_expression_aggregates (x UInt64) ENGINE = MergeTree ORDER BY max(x); -- { serverError ILLEGAL_AGGREGATION }

CREATE TABLE standalone_expression_aggregates (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE standalone_expression_aggregates ADD INDEX i (count() + 1) TYPE minmax GRANULARITY 1; -- { serverError ILLEGAL_AGGREGATION }
ALTER TABLE standalone_expression_aggregates ADD CONSTRAINT c CHECK sum(x) > 0; -- { serverError ILLEGAL_AGGREGATION }

ALTER TABLE standalone_expression_aggregates ADD CONSTRAINT c CHECK x IN (SELECT max(number) FROM numbers(10));
INSERT INTO standalone_expression_aggregates VALUES (9);
INSERT INTO standalone_expression_aggregates VALUES (1); -- { serverError VIOLATED_CONSTRAINT }

SELECT * FROM standalone_expression_aggregates;

DROP TABLE standalone_expression_aggregates;
