-- A non-NULL `Nullable(Tuple(UUID2))` partition value names the partition of its `UUID2` value.
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (u UUID1) ENGINE = MergeTree PARTITION BY u ORDER BY u;
INSERT INTO t_partition VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000000');
ALTER TABLE t_partition DROP PARTITION CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001')) AS Nullable(Tuple(UUID2)));
SELECT u FROM t_partition ORDER BY u;
DROP TABLE t_partition;

-- The same with a complex partition key.
CREATE TABLE t_partition (u UUID1, k UInt8) ENGINE = MergeTree PARTITION BY (u, k) ORDER BY u;
INSERT INTO t_partition VALUES ('00000000-0000-0000-0000-000000000001', 1), ('00000000-0000-0001-0000-000000000000', 1);
ALTER TABLE t_partition DROP PARTITION CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001'), 1) AS Nullable(Tuple(UUID2, UInt8)));
SELECT u, k FROM t_partition ORDER BY u;
DROP TABLE t_partition;

-- An identifier alias of a lambda shadows a function as the lambda does, so its argument is an ordinary string.
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda ENGINE = Memory AS
WITH (x -> x) AS identity_lambda, identity_lambda AS defaultValueOfTypeName
SELECT defaultValueOfTypeName('UUID') AS s;
SELECT s FROM t_lambda;
DROP TABLE t_lambda;
SET uuid_type_version = 1;

-- A lambda declared in the query takes precedence over a SQL UDF of the same name in `APPLY`, also in a persisted query.
DROP FUNCTION IF EXISTS review_scope_05461;
CREATE FUNCTION review_scope_05461 AS x -> x + 1;

WITH (x -> x * 3) AS review_scope_05461 SELECT * APPLY review_scope_05461 FROM values('a UInt8', 1);
WITH (x -> x * 3) AS review_scope_05461 SELECT * APPLY(x -> review_scope_05461(x)) FROM values('a UInt8', 1);

DROP TABLE IF EXISTS t_apply;
CREATE TABLE t_apply ENGINE = Memory AS
WITH (x -> x * 3) AS review_scope_05461 SELECT * APPLY review_scope_05461 FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

CREATE TABLE t_apply ENGINE = Memory AS
WITH (x -> x * 3) AS review_scope_05461 SELECT * APPLY(x -> review_scope_05461(x)) FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

-- Without a lambda of the same name, the UDF applies.
CREATE TABLE t_apply ENGINE = Memory AS SELECT * APPLY review_scope_05461 FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

DROP FUNCTION review_scope_05461;
