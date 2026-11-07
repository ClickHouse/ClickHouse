-- A number is compared with a `Bool` key of an `IN` set by its value, as `number IN (true)` and `number = true` do.

SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number IN (SELECT true);
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number NOT IN (SELECT true);
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number IN (SELECT CAST(true, 'Nullable(Bool)'));
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number IN (SELECT toLowCardinality(true));
SELECT arraySort(groupArray(x)) FROM (SELECT toNullable(number) AS x FROM numbers(4)) WHERE x IN (SELECT true);
SELECT arraySort(groupArray(x)) FROM (SELECT toLowCardinality(number) AS x FROM numbers(4)) WHERE x IN (SELECT true);
SELECT arraySort(groupArray(x)) FROM (SELECT toInt8(number) - 2 AS x FROM numbers(4)) WHERE x IN (SELECT true);
SELECT arraySort(groupArray(x)) FROM (SELECT 254 + number AS x FROM numbers(4)) WHERE x IN (SELECT true);
SELECT arraySort(groupArray(x)) FROM (SELECT number / 2 AS x FROM numbers(5)) WHERE x IN (SELECT true);
SELECT nan IN (SELECT true), toInt128(-1) IN (SELECT true), toUInt8(2) IN (SELECT true);
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE (number, 1) IN (SELECT true, 1);
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE nullIn(number, (SELECT true));
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number IN (SELECT true) SETTINGS transform_null_in = 1;
SELECT arraySort(groupArray(ifNull(toString(x), 'NULL'))) FROM (SELECT if(number = 3, NULL, number) AS x FROM numbers(4))
    WHERE x IN (SELECT CAST(NULL, 'Nullable(Bool)') UNION ALL SELECT true) SETTINGS transform_null_in = 1;
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number IN (SELECT true) SETTINGS max_bytes_before_external_set = 1;
SELECT arraySort(groupArray(number)) FROM remote('127.0.0.2', numbers(4)) WHERE number GLOBAL IN (SELECT true);

DROP TABLE IF EXISTS t_bool_set;
CREATE TABLE t_bool_set (b Bool) ENGINE = Set;
INSERT INTO t_bool_set VALUES (true);
SELECT arraySort(groupArray(number)) FROM numbers(4) WHERE number IN t_bool_set;

DROP TABLE IF EXISTS t_bool_in_key;
CREATE TABLE t_bool_in_key (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_bool_in_key SELECT number FROM numbers(4);
SELECT arraySort(groupArray(x)) FROM t_bool_in_key WHERE x IN (SELECT true) SETTINGS use_primary_key = 0;

-- A `Variant` reaches the set as a whole in a multi-column key and in the null-aware membership.
DROP TABLE IF EXISTS t_bool_in_variant;
CREATE TABLE t_bool_in_variant (x Variant(UInt64, Float64, String)) ENGINE = Memory SETTINGS allow_suspicious_variant_types = 1;
INSERT INTO t_bool_in_variant VALUES (0), (1), (2), (300), (0.5), ('true'), ('abc'), (NULL);
SELECT arraySort(groupArray(if(isNull(x), 'NULL', toString(x)))) FROM t_bool_in_variant WHERE (x, 1) IN (SELECT true, 1);
SELECT arraySort(groupArray(if(isNull(x), 'NULL', toString(x)))) FROM t_bool_in_variant WHERE (x, 1) NOT IN (SELECT true, 1);
SELECT arraySort(groupArray(if(isNull(x), 'NULL', toString(x)))) FROM t_bool_in_variant WHERE nullIn(x, (SELECT true));
SELECT arraySort(groupArray(if(isNull(x), 'NULL', toString(x)))) FROM t_bool_in_variant WHERE x IN (SELECT true) SETTINGS transform_null_in = 1;
SELECT arraySort(groupArray(if(isNull(x), 'NULL', toString(x)))) FROM t_bool_in_variant WHERE x NOT IN (SELECT true) SETTINGS transform_null_in = 1;

DROP TABLE t_bool_set;
DROP TABLE t_bool_in_key;
DROP TABLE t_bool_in_variant;
