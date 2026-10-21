#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: pyarrow is not available in fasttest builds
# With `schema_inference_mode = 'union'`, an Arrow file whose columns are all skipped by
# `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` contributes no columns, and the other
# files give the schema. A single such file still fails, with an error that explains the empty schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_PREFIX}"_*.arrow' EXIT

python3 - "$DATA_PREFIX" <<'PY'
import sys
import pyarrow as pa

batches = {
    "a": pa.record_batch([pa.array([pa.MonthDayNano([1, 2, 3])], pa.month_day_nano_interval())], names=["i"]),
    "b": pa.record_batch([pa.array([5, 6], pa.int32())], names=["n"]),
}
for name, batch in batches.items():
    with pa.OSFile(f"{sys.argv[1]}_{name}.arrow", "wb") as sink:
        with pa.ipc.new_file(sink, batch.schema) as writer:
            writer.write_batch(batch)
PY

${CLICKHOUSE_LOCAL} --query "
    SELECT n FROM file('${DATA_PREFIX}_*.arrow', Arrow) ORDER BY n
    SETTINGS schema_inference_mode = 'union', input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference = 1"

${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_PREFIX}_a.arrow', Arrow)
    SETTINGS input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference = 1" 2>&1 \
    | grep -o "may have skipped all of them" | head -1
