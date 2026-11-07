#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The schema and type cases of the Apache Iceberg verification suite, see `data_iceberg/spec/README.md`.
# Every case of `table-spec/{schema,types}/*/cases.json` is a schema or a type with a `valid` flag.
# `wrap_cases.py` turns each case into a table without snapshots: the schema cases become the table schema,
# the type cases become the type of a nullable column `c`. A valid case must be described with the
# corresponding ClickHouse types, an invalid case must be rejected with an exception.
# The `unknown` type is described as `Nullable(Nothing)`, because it has no values but NULL, and `time` as `Time64(6)`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="$CUR_DIR/data_iceberg/spec/iceberg-verification"
TABLES="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TABLES"
mkdir -p "$TABLES"
trap 'rm -rf "$TABLES"' EXIT

python3 "$SRC/wrap_cases.py" "$SRC" "$TABLES" | while read -r name valid
do
    echo "$name $valid"
    # `allow_geo_types_in_iceberg` for the cases of `types/geospatial`.
    if ! $CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 --query "DESCRIBE icebergLocal('$TABLES/$name') SETTINGS describe_compact_output = 1" 2>/dev/null
    then
        echo "exception"
    fi
done
