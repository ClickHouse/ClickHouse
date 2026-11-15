#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the dense index needs RocksDB, and the SST under test has to live on an object storage disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ensureValidDenseIndex` decides at load time whether a UNIQUE KEY part is trusted, rebuilt or
# rejected (UniqueKeyDenseIndexOps.cpp). Its promise is that a part never activates without a
# probeable dense index, and that the index is only deleted or rebuilt where that is safe:
#   * an SST that cannot be read at all may be healthy and momentarily unreachable, so it is kept
#     and the load fails with a code the caller re-raises, for a retry to recover;
#   * a part that carries no SST at all is rebuilt - unless the storage is readonly, where the
#     rebuild is impossible and the load fails closed instead;
#   * a rebuild that cannot produce an index detaches the part as broken instead of activating it.
# A part with no SST entry is the case MergeTreeData.cpp:3737 names: a UNIQUE KEY that arrived after
# the part was written. The test is one private `clickhouse-local` database, so the planted state
# belongs to the test alone and the coordination service runs inside the process.

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${work}"
mkdir -p "${work}"

cat > "${work}/cfg.xml" <<EOF
<clickhouse>
    <custom_local_disks_base_directory>${work}/</custom_local_disks_base_directory>
    <zookeeper><implementation>testkeeper</implementation></zookeeper>
</clickhouse>
EOF

run() {
    ${CLICKHOUSE_LOCAL} --config-file="${work}/cfg.xml" --path "${work}/db" --enable_unique_key 1 \
        --query "$1" < /dev/null
}

# The load failures are asserted by their error code name only: no message text, part name or size.
# A failing load is always the last statement of its instance, so the lines above it are unaffected.
failure_code() { grep -c -o -m1 'UNIQUE_KEY_DENSE_INDEX_UNREADABLE' "${work}/err"; }

# Give a table the UNIQUE KEY its part was written without, keeping the identity of the table and so
# its data directory. `$2` is the whole table definition after the name.
add_unique_key() {
    local sql="${work}/db/metadata/uk/$1.sql"
    local uuid
    uuid=$(sed -n "s/^ATTACH TABLE _ UUID '\(.*\)'$/\1/p" "${sql}")
    echo "ATTACH TABLE _ UUID '${uuid}' $2" > "${sql}"
}

run "
CREATE DATABASE uk ENGINE = Atomic;
-- A local_blob_storage disk keeps the SST in a blob of its own, which the test can take away while
-- the metadata keeps the name and the size \`checkConsistencyBase\` checks before the validation.
CREATE TABLE uk.transient (a Int32, v String) ENGINE = MergeTree ORDER BY a UNIQUE KEY (a)
SETTINGS min_bytes_for_wide_part = 0, disk = disk(
    name = d_uk,
    type = object_storage,
    object_storage_type = local_blob_storage,
    metadata_type = local,
    metadata_path = '${work}/meta/',
    path = '${work}/blobs/');
INSERT INTO uk.transient VALUES (1, 'x'), (2, 'y');
SELECT 'transient rows', count() FROM uk.transient;
-- The three parts below are written before their table has a UNIQUE KEY, so they carry no SST entry.
CREATE TABLE uk.readonly (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO uk.readonly VALUES (1), (2);
CREATE TABLE uk.missing_col (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO uk.missing_col VALUES (1), (2);
CREATE TABLE uk.duplicate_uk (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO uk.duplicate_uk VALUES (1), (1);
-- The blob name is random, so it goes to a file instead of the asserted output.
SELECT remote_path FROM system.remote_data_paths
WHERE disk_name = 'd_uk' AND local_path LIKE '%unique_key_index.sst'
INTO OUTFILE '${work}/blob' FORMAT TSVRaw;
"
blob=$(cat "${work}/blob")

# An unreadable SST: the load fails with the distinguished code and leaves the file alone, so the
# part is neither detached nor rebuilt and comes back whole once the blob is reachable again.
mv "${blob}" "${work}/sst_away"
run "SELECT count() FROM uk.transient" 2> "${work}/err"
echo -n 'transient load fails '
failure_code
mv "${work}/sst_away" "${blob}"

# No SST and no way to write one: the part must not activate unprobeable, so the load fails closed.
add_unique_key readonly \
    "(\`a\` Int32) ENGINE = MergeTree ORDER BY a UNIQUE KEY (a) SETTINGS table_readonly = 1"
# A UK column the part does not have: the rebuild cannot index it, so the part is detached as broken.
add_unique_key missing_col \
    "(\`a\` Int32, \`c\` Int32) ENGINE = MergeTree ORDER BY a UNIQUE KEY (c)"
# A part that already repeats a key cannot be indexed at all - the dense index maps one row per key.
# The rebuild throws from inside its own try block, and the part is detached as broken, not activated.
add_unique_key duplicate_uk \
    "(\`a\` Int32) ENGINE = MergeTree ORDER BY a UNIQUE KEY (a)"

# Tables are loaded on first use, so the three cases above are independent inside one instance.
run "
SELECT 'transient rows after retry', sum(rows) FROM system.parts
WHERE database = 'uk' AND table = 'transient' AND active;
SELECT 'transient detached', count() FROM system.detached_parts WHERE database = 'uk' AND table = 'transient';

SELECT 'missing column rows', count() FROM uk.missing_col;
SELECT 'missing column detached', count() FROM system.detached_parts WHERE database = 'uk' AND table = 'missing_col';

SELECT 'duplicate key rows', count() FROM uk.duplicate_uk;
SELECT 'duplicate key detached', count() FROM system.detached_parts WHERE database = 'uk' AND table = 'duplicate_uk';

SELECT count() FROM uk.readonly;
" 2> "${work}/err"
echo -n 'readonly load fails '
failure_code

rm -rf "${work}"
