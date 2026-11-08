#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# An identity partition source nested in a struct (`address.country`) that the data file does not
# store: its value is only in the manifest, and must be returned wherever the struct is read.
# The table is `data/persistent/partition_values_missing_from_files/warehouse/default/nested_source`
# of https://github.com/duckdb/duckdb-iceberg at b57f45c2c78965c71f699aa23ae73c292923668e,
# MIT License, Copyright 2018-2025 Stichting DuckDB Foundation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table location is relative (`data/persistent/...`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

$CLICKHOUSE_LOCAL -q "
SELECT * FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/nested_source') ORDER BY ALL;
SELECT count() FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/nested_source') WHERE address.country = 'fr';
SELECT id FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/nested_source') PREWHERE address = ('fr', 'lyon');
-- The condition is evaluated after the reader, not as PREWHERE.
SELECT id FROM icebergLocal('data/persistent/partition_values_missing_from_files/warehouse/default/nested_source') WHERE address = ('fr', 'lyon') SETTINGS optimize_move_to_prewhere = 0;
"
