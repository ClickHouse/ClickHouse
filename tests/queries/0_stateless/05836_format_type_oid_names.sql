-- `format_type(type_oid, typemod)` is the PostgreSQL compatibility function registered in
-- REGISTER_FUNCTION(PostgresCatalog) (src/Functions/postgresCatalog.cpp). Its FunctionDocumentation
-- there is the source of the expected behaviour: "Returns the SQL name of a type given its OID",
-- which the comment above `pgFormatType` describes as the spelling produced by PostgreSQL's
-- `format_type` over the types ClickHouse's `pg_type` emulation advertises. The modifier is decoded
-- into the precision and scale for `numeric` and into the fractional-second precision for
-- `timestamp`; an array type renders its element name with a `[]` suffix; an unknown OID is `text`.

-- Of the range types only `int4range`, `numrange` and `int8range` are checked. For the other range
-- and multirange OIDs `pgFormatType` does not agree with the PostgreSQL catalog (there 3905 is
-- `int4range[]` and `int4multirange` is 4451; 3908 is `tsrange`, 3910 is `tstzrange` and 3912 is
-- `daterange`), so the names it returns for them today are not asserted here.
SELECT '--- scalar type OIDs, no modifier';
SELECT oid, format_type(oid, -1) AS type_name
FROM
(
    SELECT arrayJoin([16, 17, 18, 19, 20, 21, 23, 25, 26, 700, 701, 1042, 1043, 1082, 1114, 1184,
                      1700, 2950, 3904, 3906, 3926, 40000]) AS oid
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
