-- An aggregate function over a `Variant` argument and an argument that is only `NULL` is replaced with `nothingNull` or
-- `nothingUInt64`. The function must be the same as the one built from its state type in the header of a remote shard.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_nothing;
CREATE TABLE t_variant_nothing (v Variant(Int128)) ENGINE = Memory;
INSERT INTO t_variant_nothing VALUES (1), (NULL), (5);

SELECT avgArgMax(v, materialize(NULL)), toTypeName(avgArgMax(v, NULL)) FROM remote('127.0.0.{1,2}', currentDatabase(), t_variant_nothing);
SELECT uniq(v, NULL), toTypeName(uniq(v, NULL)), sumIf(v, NULL), avgArgMaxIf(v, NULL, 1) FROM remote('127.0.0.{1,2}', currentDatabase(), t_variant_nothing);
SELECT avgArgMax(v, materialize(NULL)), sumOrNullDistinct(v), sumOrNull(v), min(v) IGNORE NULLS FROM remote('127.0.0.{1,2}', currentDatabase(), t_variant_nothing);
SELECT finalizeAggregation(s) FROM (SELECT avgArgMaxState(v, NULL) AS s FROM remote('127.0.0.{1,2}', currentDatabase(), t_variant_nothing));

DROP TABLE t_variant_nothing;
