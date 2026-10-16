#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"

# Table keys use the server-wide value of the setting, so `clickhouse-local` gets it on the command line.
$CLICKHOUSE_LOCAL --path "$DATA_DIR" --function_iceberg_truncate_returns_uint64_on_uint64_argument=0 -q "
CREATE TABLE tp (x UInt64) ENGINE = MergeTree PARTITION BY icebergTruncate(1000, x) ORDER BY tuple();
INSERT INTO tp VALUES (500), (9223372036854775809), (18446744073709551000);

CREATE TABLE tpn (x Nullable(UInt64)) ENGINE = MergeTree PARTITION BY icebergTruncate(1000, x) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO tpn VALUES (500), (9223372036854775809), (18446744073709551000), (NULL);

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tpl (x LowCardinality(UInt64)) ENGINE = MergeTree PARTITION BY icebergTruncate(1000, x) ORDER BY tuple();
INSERT INTO tpl VALUES (500), (9223372036854775809), (18446744073709551000);

CREATE TABLE ts (x UInt64) ENGINE = MergeTree ORDER BY icebergTruncate(1000, x) SETTINGS index_granularity = 1;
INSERT INTO ts SELECT number * 1000 FROM numbers(5);
INSERT INTO ts VALUES (18446744073709551000), (18446744073709550000);
OPTIMIZE TABLE ts FINAL;

CREATE TABLE tk (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO tk SELECT number * 1000 FROM numbers(10);

CREATE TABLE ti (x UInt64, INDEX i icebergTruncate(1000, x) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO ti SELECT number * 1000 FROM numbers(5);
INSERT INTO ti VALUES (18446744073709551000), (18446744073709550000);
OPTIMIZE TABLE ti FINAL;
"

# Reopen the same parts with the previous behavior selected by `compatibility`.
$CLICKHOUSE_LOCAL --path "$DATA_DIR" --compatibility=26.10 -q "
SELECT count() FROM system.detached_parts WHERE database = currentDatabase();
SELECT DISTINCT _partition_id FROM tp ORDER BY 1;
SELECT count() FROM tp WHERE x > 9000000000000000000;
SELECT count() FROM tpn WHERE x > 9000000000000000000;
SELECT count() FROM tpl WHERE x > 9000000000000000000;
SELECT count() FROM ts WHERE x >= 1000;
SELECT count() FROM ts WHERE icebergTruncate(1000, x) < 0;
SELECT count() FROM tk WHERE icebergTruncate(1000, x) = 3000 SETTINGS max_rows_to_read = 3;
SELECT count() FROM ti WHERE icebergTruncate(1000, x) < 0 SETTINGS force_data_skipping_indices = 'i', use_skip_indexes_on_data_read = 0, max_rows_to_read = 2;
"

rm -rf "$DATA_DIR"
