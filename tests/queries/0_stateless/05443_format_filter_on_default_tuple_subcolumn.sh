#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: pyarrow and the ORC format are not available in fasttest builds
# A NULL struct read into `t Tuple(x Int64) DEFAULT tuple(-1)` becomes `(-1)` after the format, so a filter on the
# subcolumn `t.x` must not reach the format, which would prune by the values stored in the file.
# Only ORC is tested: the Parquet reader doesn't apply the tuple default to a NULL struct, so its baseline differs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_PREFIX}".orc' EXIT

python3 - "$DATA_PREFIX" <<'PY'
import sys
import pyarrow as pa
import pyarrow.orc as orc

t = pa.array([None if i % 2 == 0 else {"x": 1} for i in range(10)], pa.struct([("x", pa.int64())]))
table = pa.table({"t": t})
orc.write_table(table, f"{sys.argv[1]}.orc")
PY

for format in ORC; do
    format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')
    for push_down in 1 0; do
        ${CLICKHOUSE_LOCAL} --query "
            SELECT count() FROM file('${DATA_PREFIX}.${format_lower}', ${format}, 't Tuple(x Int64) DEFAULT tuple(-1)')
            WHERE t.x = -1
            SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1,
                input_format_${format_lower}_filter_push_down = ${push_down}"
    done
    ${CLICKHOUSE_LOCAL} --query "
        SELECT countIf(t.x = -1) FROM file('${DATA_PREFIX}.${format_lower}', ${format}, 't Tuple(x Int64) DEFAULT tuple(-1)')
        SETTINGS input_format_null_as_default = 1, input_format_defaults_for_omitted_fields = 1"
done
