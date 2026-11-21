-- A `DEFAULT` written inside a `Tuple` alternative of a `Variant` is pulled up as a `CAST` to that
-- alternative. The column type gets the current aggregate-function state version pinned, so an
-- unversioned `AggregateFunction` inside the alternative has to be spelled with the same version in
-- the `CAST`, otherwise the conversion to the `Variant` fails with `CANNOT_CONVERT_TYPE`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_variant_state;

CREATE TABLE t_tuple_default_variant_state
(
    id UInt8,
    c Variant(UInt64, Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64), a UInt8 DEFAULT 1))
)
ENGINE = Memory;

ALTER TABLE t_tuple_default_variant_state
    ADD COLUMN d Variant(UInt64, Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64), a UInt8 DEFAULT 2));

INSERT INTO t_tuple_default_variant_state (id) VALUES (1);
SELECT variantType(c), tupleElement(variantElement(c, 'Tuple(s AggregateFunction(1, quantileDeterministic, UInt64, UInt64), a UInt8)'), 'a'),
       variantType(d), tupleElement(variantElement(d, 'Tuple(s AggregateFunction(1, quantileDeterministic, UInt64, UInt64), a UInt8)'), 'a')
FROM t_tuple_default_variant_state;

DROP TABLE t_tuple_default_variant_state;
