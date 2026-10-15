-- A `Variant` or `Dynamic` constant evaluated by the `VALUES` expression fallback keeps the layout of its active
-- `UUID` / `UUID2` alternative when it is inserted into the other UUID type.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET input_format_values_deduce_templates_of_expressions = 0;
SET allow_suspicious_variant_types = 1;

CREATE TABLE t1 (u UUID1) ENGINE = Memory;
INSERT INTO t1 VALUES (CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID2, String)));
INSERT INTO t1 VALUES (CAST(toUUID2('00000000-0000-0000-0000-000000000002') AS Dynamic));
INSERT INTO t1 VALUES (CAST('00000000-0000-0000-0000-000000000003' AS Variant(UUID2, String)));
SELECT toString(u) FROM t1 ORDER BY toString(u);

CREATE TABLE t2 (u UUID2) ENGINE = Memory;
INSERT INTO t2 VALUES (CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String)));
INSERT INTO t2 VALUES (CAST(toUUID('00000000-0000-0000-0000-000000000002') AS Dynamic));
SELECT toString(u) FROM t2 ORDER BY u;

-- Nested sources and destinations.
CREATE TABLE t3 (a Array(UUID1), n Nullable(UUID2)) ENGINE = Memory;
INSERT INTO t3 VALUES (
    [CAST(toUUID2('00000000-0000-0000-0000-000000000004') AS Variant(UUID2, String))],
    CAST(toUUID('00000000-0000-0000-0000-000000000005') AS Variant(UUID, String)));
INSERT INTO t3 VALUES ([], CAST(NULL AS Variant(UUID, String)));
SELECT arrayMap(x -> toString(x), a), toString(n) FROM t3 ORDER BY length(a) DESC;

-- A `Dynamic` destination keeps the type of the value.
CREATE TABLE t4 (d Dynamic) ENGINE = Memory;
INSERT INTO t4 VALUES (CAST(toUUID2('00000000-0000-0000-0000-000000000006') AS Dynamic));
INSERT INTO t4 VALUES (CAST(toUUID2('00000000-0000-0000-0000-000000000007') AS Variant(UUID2, String)));
SELECT toString(d), dynamicType(d) FROM t4 ORDER BY toString(d);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
