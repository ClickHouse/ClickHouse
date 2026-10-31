-- An element without an explicit `DEFAULT` is filled with `defaultValueOfTypeName`. The column type gets
-- the current aggregate-function state version pinned, so an unversioned `AggregateFunction` inside a
-- `Variant` alternative of such an element has to be spelled with the same version in the generated
-- type name, otherwise the conversion to the `Variant` fails with `CANNOT_CONVERT_TYPE`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_unfilled_state;

CREATE TABLE t_tuple_default_unfilled_state
(
    id UInt8,
    c Tuple(
        v Variant(UInt64, Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64), a UInt8)),
        b UInt8 DEFAULT 1)
)
ENGINE = Memory;

ALTER TABLE t_tuple_default_unfilled_state
    ADD COLUMN d Tuple(
        v Variant(UInt64, Tuple(s AggregateFunction(quantileDeterministic, UInt64, UInt64), a UInt8)),
        b UInt8 DEFAULT 2);

INSERT INTO t_tuple_default_unfilled_state (id) VALUES (1);
SELECT variantType(c.v), c.b, variantType(d.v), d.b FROM t_tuple_default_unfilled_state;

DROP TABLE t_tuple_default_unfilled_state;
