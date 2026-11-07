#!/usr/bin/env bash

# The hash vectors of the Iceberg `bucket` transform (`table-spec/transforms/bucket/cases.jsonl` of
# sungwy/iceberg-testing, the vectors of Appendix B of the Iceberg table specification and boundary cases),
# see `data_iceberg/spec/README.md`. `bucket[N](x) = (hash(x) & 0x7fffffff) % N`, so with N = 2147483647
# `icebergBucket` gives the hash with the sign bit cleared (unless it is 0x7fffffff).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CASES="$CUR_DIR/data_iceberg/spec/iceberg-testing/bucket/cases.jsonl"

CASES_TSV=$($CLICKHOUSE_LOCAL --query "SELECT id, type, value, hash FROM file('$CASES', JSONEachRow, 'id String, type String, value String, hash Int32') FORMAT TSV")

while IFS=$'\t' read -r id type value hash
do
    case "$type" in
        int) expr="toInt32('$value')" ;;
        long) expr="toInt64('$value')" ;;
        decimal*) expr="toDecimal32('$value', 2)" ;;
        date) expr="toDate32('$value')" ;;
        # Iceberg `time` is microseconds since midnight. `icebergBucket` does not support `Time64` yet.
        time) expr="toTime64('$value', 6)" ;;
        timestamp) expr="parseDateTime64BestEffort('$value', 6, 'UTC')" ;;
        string) expr="'$value'" ;;
        uuid) expr="toUUID('$value')" ;;
        fixed*) expr="toFixedString(unhex('$value'), 4)" ;;
        binary) expr="unhex('$value')" ;;
    esac
    echo "$id $($CLICKHOUSE_LOCAL --enable_time_time64_type 1 --query "SELECT icebergBucket(2147483647, $expr) = bitAnd($hash, 2147483647) % 2147483647" 2>&1 | head -n1)"
done <<< "$CASES_TSV"
