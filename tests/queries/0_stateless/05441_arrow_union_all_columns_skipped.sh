#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: pyarrow is not available in fasttest builds
# With `schema_inference_mode = 'union'`, an Arrow file whose columns are all skipped by
# `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` contributes no columns, and the other
# files give the schema.
# `data_arrow/unknown_type.arrows` is a schema-only stream whose single field has a type id that no Arrow version
# defines, so the reader skips it (see 05408_arrow_infer_null_type). A single such file still fails, with an error that
# explains the empty schema, and that is checked there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_PREFIX}"_*.arrows' EXIT

cp "$CUR_DIR"/data_arrow/unknown_type.arrows "${DATA_PREFIX}_a.arrows"

python3 - "$DATA_PREFIX" <<'PY'
import sys
import pyarrow as pa

batch = pa.record_batch([pa.array([5, 6], pa.int32())], names=["n"])
with pa.OSFile(f"{sys.argv[1]}_b.arrows", "wb") as sink:
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
PY

${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_PREFIX}_*.arrows', ArrowStream)
    SETTINGS schema_inference_mode = 'union', input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference = 1"
