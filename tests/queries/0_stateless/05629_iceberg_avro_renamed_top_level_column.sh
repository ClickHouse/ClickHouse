#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# The top-level fields of an Iceberg Avro data file are read by field id, as Parquet and ORC read them: a renamed column, a renamed
# struct and a column renamed to the name of a dropped one keep their values when the file is read under a later schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

${CLICKHOUSE_LOCAL} -q "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t (id Int32, a Nullable(Int32), c Nullable(Int32), d Nullable(Int32), e Nullable(Int32), s0 Tuple(x Nullable(Int32)))
        ENGINE = IcebergLocal('${TMP_DIR}/t/', 'Avro');
    INSERT INTO t VALUES (1, 100, 55, 66, 77, tuple(7));
    ALTER TABLE t RENAME COLUMN a TO b;
    ALTER TABLE t DROP COLUMN c;
    ALTER TABLE t ADD COLUMN c Nullable(Int32);
    ALTER TABLE t DROP COLUMN d;
    ALTER TABLE t RENAME COLUMN e TO d;
    ALTER TABLE t RENAME COLUMN s0 TO s;
"

# Every snapshot records the current schema, as one committed across the ALTERs does.
python3 - "${TMP_DIR}/t/metadata" <<'PY'
import json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = metadata["current-schema-id"]
metadata["last-updated-ms"] += 1
with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY

${CLICKHOUSE_LOCAL} -q "
    SELECT * FROM icebergLocal('${TMP_DIR}/t/', 'Avro');
    SELECT * FROM icebergLocal('${TMP_DIR}/t/');
    SELECT d, s.x FROM icebergLocal('${TMP_DIR}/t/', 'Avro');
    SELECT count() FROM icebergLocal('${TMP_DIR}/t/', 'Avro') WHERE b = 100;
    SELECT count() FROM icebergLocal('${TMP_DIR}/t/', 'Avro') WHERE c IS NULL AND d = 77;
"
