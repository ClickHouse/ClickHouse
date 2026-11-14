#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# An Iceberg v3 table with a `variant` column, written by Spark 4.1 with Iceberg 1.10.1: 10 rows in three
# Parquet files (all values non-null, nulls among values, all nulls). The table and the expected
# results come from the Presto Iceberg connector test `TestIcebergVariantTypeRead`, which reads the
# variant as JSON, see `data_iceberg/presto/README.md`.
#
# The manifests have no `partition-spec`, `partition-spec-id` and `schema` keys in the Avro file
# header, so ClickHouse rejects them with `No partition-spec in iceberg manifest file` until
# https://github.com/ClickHouse/ClickHouse/issues/129989 is fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/presto"

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`, so the table
# is copied into a uniquely named subdirectory of the working directory.
WORK="./${CLICKHOUSE_TEST_UNIQUE_NAME}_presto"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT
WORK=$(realpath "$WORK")

# Presto's tests replace the `FILEPATH` placeholder in the metadata with the absolute path of
# a copy of the table; do the same, see `substitute_filepath.py`.
python3 -I "$DATA/substitute_filepath.py" "$DATA/presto-iceberg/variant_type_read" "$WORK/variant_type_read"

$CLICKHOUSE_LOCAL -q "
DESCRIBE icebergLocal('$WORK/variant_type_read');
SELECT count() FROM icebergLocal('$WORK/variant_type_read');

SELECT 'all rows';
SELECT id, data FROM icebergLocal('$WORK/variant_type_read') ORDER BY id;
SELECT id, data FROM icebergLocal('$WORK/variant_type_read') WHERE id IN (8, 9, 10) ORDER BY id;

CREATE VIEW v AS SELECT id, data,
    data.JSON.user::Nullable(String) AS user_name,
    data.JSON.age::Nullable(Int32) AS age,
    data.JSON.active::Nullable(Bool) AS active
FROM icebergLocal('$WORK/variant_type_read');

SELECT 'fields';
SELECT id, user_name, age, active FROM v ORDER BY id;
SELECT id, user_name, data.JSON.tags, data.JSON.scores FROM v ORDER BY id;

SELECT 'unnest';
SELECT id, user_name, arrayJoin(data.JSON.tags::Array(String)) AS tag FROM v WHERE data.JSON.tags IS NOT NULL ORDER BY id, tag;
SELECT id, user_name, arrayJoin(data.JSON.scores::Array(Int32)) AS score FROM v WHERE data.JSON.scores IS NOT NULL ORDER BY id, score;

SELECT 'filters';
SELECT id FROM v WHERE age > 28 ORDER BY id;
SELECT id, user_name FROM v WHERE active ORDER BY id;

SELECT 'aggregation';
SELECT user_name, count() FROM v GROUP BY user_name ORDER BY user_name NULLS LAST;
SELECT user_name, avg(age), min(age), max(age) FROM v GROUP BY user_name ORDER BY user_name NULLS LAST;
SELECT user_name, count() AS c FROM v GROUP BY user_name HAVING c > 1 ORDER BY user_name NULLS LAST;
SELECT uniqExact(user_name), uniqExact(age) FROM v;

SELECT 'joins';
SELECT t1.id, t1.user_name, t2.id, t2.user_name FROM v AS t1 JOIN v AS t2 ON t1.user_name = t2.user_name AND t1.id < t2.id ORDER BY t1.id, t2.id;
SELECT t1.id, t1.user_name, t2.id, t2.user_name, t1.age FROM v AS t1 JOIN v AS t2 ON t1.age = t2.age AND t1.id < t2.id ORDER BY t1.age, t1.id;
"
