-- The element types of a `Tuple` with element `DEFAULT`s are kept as strings, as the arguments of
-- `defaultValueOfTypeName` and of the `CAST` for a `Variant` alternative. These strings must not be
-- produced by `formatForLogging`, which runs the server's `query_masking_rules` over them: the rules in
-- `tests/config/config.d/query_masking_rules.xml` replace `TOPSECRET.TOPSECRET` with `[hidden]`, which
-- turned the types below into an `Enum8` with two elements of the same name.

DROP TABLE IF EXISTS t_tuple_default_masked;

CREATE TABLE t_tuple_default_masked
(
    id UInt8,
    c Tuple(e Enum8('TOPSECRET.TOPSECRET' = 1, '[hidden]' = 2), x UInt8 DEFAULT 7),
    v Variant(UInt64, Tuple(e Enum8('TOPSECRET.TOPSECRET' = 1, '[hidden]' = 2) DEFAULT '[hidden]', y UInt8 DEFAULT 8))
)
ENGINE = Memory;

INSERT INTO t_tuple_default_masked (id) VALUES (1);

SELECT c.e = 'TOPSECRET.TOPSECRET', c.x, v FROM t_tuple_default_masked;

DROP TABLE t_tuple_default_masked;
