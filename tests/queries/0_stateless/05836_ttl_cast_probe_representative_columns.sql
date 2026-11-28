-- A typed `CAST` to a `Dynamic`/`Variant` carrier inside a TTL expression is probed at DDL time: the
-- validator executes the cast on synthetic single-row columns built by `makeRepresentativeColumns`
-- (src/Storages/TTLDescription.cpp) and checks the consumers of the cast against the union of the
-- outputs. A `Variant` source needs one representative per alternative and a `Tuple` source the
-- cartesian product of its elements, with at most 256 probe combinations before the validator gives up
-- with `BAD_TTL_EXPRESSION` -- see the message of that limit and the escape hatch
-- `allow_suspicious_ttl_expressions` described in src/Core/Settings.cpp.

SET allow_suspicious_low_cardinality_types = 1;
-- The escape hatch must stay off, it skips the TTL validator and with it the probe.
SET allow_suspicious_ttl_expressions = 0;

DROP TABLE IF EXISTS ttl_probe_tuple;
DROP TABLE IF EXISTS ttl_probe_map;
DROP TABLE IF EXISTS ttl_probe_lc;
DROP TABLE IF EXISTS ttl_probe_variant_ok;

-- A `Tuple` source: one set of representatives per element (2 for the `Variant`, 1 for the `Int32`),
-- probed over their cartesian product. The cast itself is dead weight inside `ignore(...)`, so the TTL
-- is `d + 1 day` and the row below, dated far in the future, stays readable.
CREATE TABLE ttl_probe_tuple (d Date, t Tuple(Variant(String, UInt64), Int32))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(1 + ignore(CAST(t, 'Dynamic')));
INSERT INTO ttl_probe_tuple VALUES ('2100-01-01', (1, 5));
SELECT count() FROM ttl_probe_tuple;
SELECT t.2, toString(t.1) FROM ttl_probe_tuple;

-- A `Map` source: the representative is a one-element map, built by recursing through the `Array` and
-- the `Tuple` of the nested type. The plain default map is empty, which would hide the element types.
CREATE TABLE ttl_probe_map (d Date, m Map(String, Variant(String, UInt64)))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(1 + ignore(CAST(m, 'Dynamic')));
INSERT INTO ttl_probe_map VALUES ('2100-01-01', map('a', 7));
SELECT count() FROM ttl_probe_map;
SELECT toString(m['a']) FROM ttl_probe_map;

-- A `LowCardinality` source: the representative keeps the dictionary wrapper. The dictionary is
-- numeric on purpose, a `LowCardinality(String)` source stays on the string-inference path, which
-- never reaches the probe at all.
CREATE TABLE ttl_probe_lc (d Date, l LowCardinality(UInt32))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(1 + ignore(CAST(l, 'Dynamic')));
INSERT INTO ttl_probe_lc VALUES ('2100-01-01', 11);
SELECT count() FROM ttl_probe_lc;
SELECT l FROM ttl_probe_lc;

-- The probe narrows the domain of the cast result to the payloads the alternatives of the source can
-- store, so `length` over the result is accepted when every alternative has a length ...
CREATE TABLE ttl_probe_variant_ok (d Date, v Variant(String, Array(UInt8)))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(assumeNotNull(length(CAST(v, 'Dynamic'))));
INSERT INTO ttl_probe_variant_ok VALUES ('2100-01-01', 'abc'), ('2100-01-01', [1, 2]);
SELECT count() FROM ttl_probe_variant_ok;
SELECT toString(v) FROM ttl_probe_variant_ok ORDER BY toString(v);

-- ... and rejected as soon as one alternative has none: a row storing the `UInt32` alternative would
-- fail during TTL execution. A single representative per source would wrongly accept this table.
CREATE TABLE ttl_probe_variant_bad (d Date, v Variant(String, UInt32))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(assumeNotNull(length(CAST(v, 'Dynamic')))); -- { serverError BAD_TTL_EXPRESSION }

-- More probe combinations than the limit: 2^9 element combinations of a `Tuple` ...
CREATE TABLE ttl_probe_tuple_overflow (d Date, t Tuple(
    Variant(String, UInt64), Variant(String, UInt64), Variant(String, UInt64),
    Variant(String, UInt64), Variant(String, UInt64), Variant(String, UInt64),
    Variant(String, UInt64), Variant(String, UInt64), Variant(String, UInt64)))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(1 + ignore(CAST(t, 'Dynamic'))); -- { serverError BAD_TTL_EXPRESSION }

-- ... and 2^8 + 1 representatives of a `Variant` whose `Tuple` alternative alone fills the budget.
CREATE TABLE ttl_probe_variant_overflow (d Date, v Variant(String, Tuple(
    Variant(String, UInt64), Variant(String, UInt64), Variant(String, UInt64),
    Variant(String, UInt64), Variant(String, UInt64), Variant(String, UInt64),
    Variant(String, UInt64), Variant(String, UInt64))))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(1 + ignore(CAST(v, 'Dynamic'))); -- { serverError BAD_TTL_EXPRESSION }
