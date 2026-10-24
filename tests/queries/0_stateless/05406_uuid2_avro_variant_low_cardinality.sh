#!/usr/bin/env bash
# Tags: no-fasttest
# The Avro reader matches the branches of a union to the alternatives of a `Variant` by the branch types inferred with
# the `UUID2` paths from the file metadata, so a branch goes to the alternative it was written from even when the
# order of the alternatives changes on readback (here `LowCardinality` is dropped and the alternatives are re-sorted).
# See https://github.com/ClickHouse/ClickHouse/pull/110084

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    SELECT number = 0
        ? tuple(toLowCardinality(toUUID2('00112233-4455-6677-8899-aabbccddeeff')))::Tuple(x LowCardinality(UUID2))::Variant(Tuple(x LowCardinality(UUID2)), Tuple(x UUID))
        : tuple(toUUID('00112233-4455-6677-8899-aabbccddeeff'))::Tuple(x UUID)::Variant(Tuple(x LowCardinality(UUID2)), Tuple(x UUID)) AS v
    FROM numbers(2)
    FORMAT Avro SETTINGS allow_suspicious_variant_types = 1" \
    | $CLICKHOUSE_LOCAL --input-format Avro --table t -q "
        DESCRIBE TABLE t SETTINGS describe_compact_output = 1;
        SELECT v, variantType(v), variantElement(v, 'Tuple(x UUID2)') FROM t"

echo "-- explicit structure with the other UUID type"
$CLICKHOUSE_LOCAL -q "
    SELECT toUUID2('00112233-4455-6677-8899-aabbccddeeff')::Variant(UUID2, Int64) AS v
    FORMAT Avro SETTINGS allow_suspicious_variant_types = 1" \
    | $CLICKHOUSE_LOCAL --input-format Avro --structure 'v Variant(UUID, Int64)' --table t -q "SELECT v, variantType(v) FROM t"
