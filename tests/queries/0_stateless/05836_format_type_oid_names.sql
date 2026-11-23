-- `format_type(type_oid, typemod)` is the PostgreSQL compatibility function registered in
-- REGISTER_FUNCTION(PostgresCatalog) (src/Functions/postgresCatalog.cpp). Its FunctionDocumentation
-- there is the source of the expected behaviour: "Returns the SQL name of a type given its OID",
-- which the comment above `pgFormatType` describes as the spelling produced by PostgreSQL's
-- `format_type` over the types ClickHouse's `pg_type` emulation advertises. The modifier is decoded
-- into the precision and scale for `numeric` and into the fractional-second precision for
-- `timestamp`; an array type renders its element name with a `[]` suffix; an unknown OID is `text`.

SELECT '--- scalar type OIDs, no modifier';
SELECT oid, format_type(oid, -1) AS type_name
FROM
(
    SELECT arrayJoin([16, 17, 19, 20, 21, 23, 25, 26, 700, 701, 1043, 1082, 1114, 1184,
                      1700, 2950, 3904, 3905, 3906, 3907, 3910, 3911, 3912, 3913, 3914, 3915,
                      3926, 3927, 40000]) AS oid
)
ORDER BY oid;

-- The OIDs 18 and 1042 are both rendered as `character` by `pgFormatType`, which does not match
-- PostgreSQL: OID 18 is PostgreSQL's internal `"char"` type, and a bare OID 1042 (no modifier) is
-- `bpchar` there, with `character(n)` reserved for the case where a modifier is present
-- (postgres src/include/catalog/pg_type.dat, src/backend/utils/adt/format_type.c). The spelling of
-- these two OIDs is therefore deliberately not pinned here, so that a later compatibility fix of
-- `pgFormatType` does not have to change this test. Asserted is only that both OIDs are mapped,
-- i.e. that they do not fall through to the unknown-OID fallback `text`.
SELECT '--- OIDs 18 and 1042 are mapped (their spelling is not pinned, see the comment)';
SELECT oid, format_type(oid, -1) != 'text' AS is_mapped
FROM
(
    SELECT arrayJoin([18, 1042]) AS oid
)
ORDER BY oid;

SELECT '--- array type OIDs render the element name with a [] suffix';
SELECT oid, format_type(oid, -1) AS type_name
FROM
(
    SELECT arrayJoin([1000, 1005, 1007, 1009, 1016, 1021, 1022, 1115, 1182, 1231, 2951]) AS oid
)
ORDER BY oid;

SELECT '--- an OID the server does not know is rendered as text';
SELECT format_type(99999, -1), format_type(0, -1), format_type(42, -1);

SELECT '--- the modifier carries the timestamp precision and the numeric precision and scale';
SELECT format_type(1114, 3), format_type(1184, 6), format_type(1115, 0);
SELECT format_type(1700, 1310726), format_type(1231, 1310724), format_type(1700, 3);
