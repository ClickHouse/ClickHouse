#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The children of an Arrow union restart the dotted path, so the struct child `s` of a union child has the path
# `s`, which is also the name of an unrelated column of the header. The requested type of that column must not
# change how the union child decodes: the union declares its children from the Arrow schema. Before the fix the
# child was decoded under the type of `s` (`IPv6`, or a `Tuple` or `Nullable` type that keeps the null map of a
# struct) while still declared as the schema says, and the cast of the union read its column as another type.

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_LOCAL -q "
    SELECT CAST(CAST(tuple(unhex('20010db800000000000000000000000' || toString(number + 1))) AS Tuple(s String)) AS Variant(Tuple(s String), UInt64)) AS u,
        [u] AS a,
        unhex('20010db800000000000000000000000' || toString(number + 1)) AS s
    FROM numbers(3)
    INTO OUTFILE '${DATA}.union' TRUNCATE FORMAT ArrowStream"

# The struct child of the union is a nullable struct.
$CLICKHOUSE_LOCAL -q "
    SELECT CAST(CAST(tuple(if(number = 1, NULL, CAST(tuple(number) AS Nullable(Tuple(x UInt64))))) AS Tuple(s Nullable(Tuple(x UInt64)))) AS Variant(Tuple(s Nullable(Tuple(x UInt64))), String)) AS u,
        CAST(tuple(number) AS Tuple(x UInt64)) AS s
    FROM numbers(3)
    INTO OUTFILE '${DATA}.nullable' TRUNCATE FORMAT ArrowStream SETTINGS allow_experimental_nullable_tuple_type = 1"

echo "-- union String and unrelated column IPv6"
$CLICKHOUSE_LOCAL -q "SELECT hex(u), s FROM file('${DATA}.union', ArrowStream, 'u String, s IPv6')"
echo "-- union inside a list"
$CLICKHOUSE_LOCAL -q "SELECT hex(a), s FROM file('${DATA}.union', ArrowStream, 'a String, s IPv6')"
echo "-- union Variant and unrelated column IPv6"
$CLICKHOUSE_LOCAL -q "SELECT variantType(u), hex(CAST(u AS String)), s FROM file('${DATA}.union', ArrowStream, 'u Variant(Tuple(s String), UInt64), s IPv6')"
echo "-- nullable struct in a union and unrelated column Tuple"
$CLICKHOUSE_LOCAL -q "SELECT u, s FROM file('${DATA}.nullable', ArrowStream, 'u String, s Tuple(x UInt64)') SETTINGS enable_nullable_tuple_type = 0"
echo "-- nullable struct in a union and unrelated column Nullable"
$CLICKHOUSE_LOCAL -q "SELECT u, s FROM file('${DATA}.nullable', ArrowStream, 'u String, s Nullable(String)') SETTINGS enable_nullable_tuple_type = 0"

rm -f "${DATA}.union" "${DATA}.nullable"
