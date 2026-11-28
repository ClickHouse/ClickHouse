#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Incomplete inferred types (`Nullable(Nothing)` for `\N`, `Nothing` for `NULL` with non-nullable inference)
# must not be validated with the reader; schema finalization turns them into strings.
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(a), a FROM format(Form, 'a=%5CN')"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(b), b FROM format(Form, 'a=1&b=%5CN')"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(a), a FROM format(Form, 'a=NULL') SETTINGS schema_inference_make_columns_nullable = 0"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(a), a FROM format(Form, 'a=%5BNULL%5D') SETTINGS schema_inference_make_columns_nullable = 0"

# The inferred type depends on `input_format_null_as_default`, so it must be a part of the schema cache key.
DATA_FILE="${CLICKHOUSE_TMP:?}/form_cache.form"
printf 'a=text&b=%%5BNULL%%2C42%%5D' > "$DATA_FILE"

$CLICKHOUSE_LOCAL -q "
SET schema_inference_make_columns_nullable = 0;
SET input_format_null_as_default = 1;
DESCRIBE file('$DATA_FILE', Form);
SET input_format_null_as_default = 0;
DESCRIBE file('$DATA_FILE', Form);
SELECT * FROM file('$DATA_FILE', Form);
"

rm -f "$DATA_FILE"
