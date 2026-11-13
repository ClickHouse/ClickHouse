#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO build option).

# A reader selects the columns of an Iceberg data file by field id (https://iceberg.apache.org/spec/#column-projection),
# also when the file is read under a later schema than the one it was written with. A snapshot records such a schema
# when its writer commits across a schema change, or once the snapshot that added the file is expired after its
# manifest was rewritten under the later schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for format in ORC Avro Parquet; do
    TABLE="t_${CLICKHOUSE_DATABASE}_${format}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
    rm -rf "${TABLE_PATH}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${TABLE} (id Int32, a Nullable(Int32), c Nullable(Int32), d Nullable(Int32), e Nullable(Int32), f Nullable(Int32), s0 Tuple(x Nullable(Int32)), g Nullable(Int32))
            ENGINE = IcebergLocal('${TABLE_PATH}/', '${format}');
        INSERT INTO ${TABLE} VALUES (1, 100, 55, 66, 77, 88, tuple(7), 99);
        ALTER TABLE ${TABLE} RENAME COLUMN a TO b;
        ALTER TABLE ${TABLE} DROP COLUMN c;
        ALTER TABLE ${TABLE} ADD COLUMN c Nullable(Int32);
        ALTER TABLE ${TABLE} DROP COLUMN d;
        ALTER TABLE ${TABLE} RENAME COLUMN e TO d;
        ALTER TABLE ${TABLE} RENAME COLUMN s0 TO s;
        ALTER TABLE ${TABLE} RENAME COLUMN f TO F;
        ALTER TABLE ${TABLE} ADD COLUMN f Nullable(Int32);
    "
    # Every snapshot records the current schema, as one committed across the ALTERs does. ALTER cannot rename `g` to `s.y`.
    python3 - "${TABLE_PATH}/metadata" <<'PY'
import json, os, re, sys
metadata_dir = sys.argv[1]
version = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
    metadata = json.load(f)
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = metadata["current-schema-id"]
schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
next(field for field in schema["fields"] if field["name"] == "g")["name"] = "s.y"
with open(os.path.join(metadata_dir, f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY
    echo "${format}"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT id, b, c, d, s FROM icebergLocal('${TABLE_PATH}/', '${format}');
        SELECT countIf(b = 100), countIf(c = 55), countIf(c IS NULL), countIf(d = 66), countIf(d = 77), countIf(s.x = 7) FROM icebergLocal('${TABLE_PATH}/', '${format}');
        SELECT count() FROM icebergLocal('${TABLE_PATH}/', '${format}') WHERE c IS NULL;
        SELECT count() FROM icebergLocal('${TABLE_PATH}/', '${format}') WHERE d = 77;
        SELECT F, f, \`s.y\` FROM icebergLocal('${TABLE_PATH}/', '${format}');
        SELECT count() FROM icebergLocal('${TABLE_PATH}/', '${format}') WHERE \`s.y\` = 99;
        SELECT f, F FROM icebergLocal('${TABLE_PATH}/', '${format}') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
        SELECT count() FROM icebergLocal('${TABLE_PATH}/', '${format}') WHERE f IS NULL SETTINGS input_format_orc_case_insensitive_column_matching = 1;
    "
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH}"
done

echo "ORC without field ids"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_05629.orc', 'ORC', 's Tuple(y Nullable(Int32)), \`s.x\` Nullable(Int32)') SELECT tuple(5), 100 SETTINGS engine_file_truncate_on_insert = 1;
    SELECT \`s.x\` FROM file('${CLICKHOUSE_DATABASE}_05629.orc');
"
rm -f "${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05629.orc"
