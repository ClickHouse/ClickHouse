#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A disk definition that uses `from_env` is stored with credentials markers only if the resolved disk relies on server
# credentials, so with `T=local` it is stored without them. Older versions stored it without them (26.3), with
# `_server_credentials_allowed` only (26.7 to 26.10), or with both markers (26.11), because they took any definition with
# `from_env` for one that may use server credentials. After an upgrade, declaring the same disk again in a new table must
# reuse the existing disk, and the new table must load after a restart.

export T=local

# `sed` expressions that make a stored disk definition look as if an older version had written it: the markers follow the
# last argument, the path.
add_s3_marker="s/\(path = '[^']*'\))/\1, equals(_server_credentials_allowed, 1))/g"
add_both_markers="s/\(path = '[^']*'\))/\1, equals(_server_credentials_allowed, 1), equals(_azure_server_credentials_allowed, 1))/g"

run_case()
{
    local name=$1 edit=$2 type=${3:-"'from_env T'"}
    local dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${name}"
    rm -rf "${dir}"
    local named="disk(name = 'named_disk', type = ${type}, path = '${dir}/disk_named/')"
    local unnamed="disk(type = ${type}, path = '${dir}/disk_unnamed/')"

    run() { ${CLICKHOUSE_LOCAL} --path "${dir}/data" --dynamic_disk_allow_from_env=1 -mn -q "$1" -- --custom_local_disks_base_directory="${dir}/" 2>&1 | grep -oE '^[0-9]+$|^[a-z0-9_]+$|\([A-Z_]+\)'; }
    disks="SELECT if(name = 'named_disk', name, 'unnamed') FROM system.disks WHERE name = 'named_disk' OR name LIKE '__tmp_internal_%' ORDER BY name DESC"

    echo "--- ${name}"
    run "CREATE TABLE a (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${named}; INSERT INTO a VALUES (1);
         CREATE TABLE b (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${unnamed}; INSERT INTO b VALUES (1);"
    # Make the stored definitions look as if an older version had written them.
    sed -i "${edit}" "${dir}"/data/store/*/*/{a,b}.sql

    echo "reuse after upgrade"
    run "CREATE TABLE a2 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${named}; SELECT 'named_ok'"
    run "CREATE TABLE b2 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${unnamed}; SELECT 'unnamed_ok'"
    run "INSERT INTO a2 VALUES (2); INSERT INTO b2 VALUES (2); ${disks}"

    echo "the new tables store the definition of the existing tables"
    for t in a b; do
        cmp -s <(grep -o "disk = disk(.*)" "${dir}"/data/store/*/*/${t}.sql) <(grep -o "disk = disk(.*)" "${dir}"/data/store/*/*/${t}2.sql) && echo "${t}2 same as ${t}"
    done

    echo "after restart"
    run "SELECT count() FROM a; SELECT count() FROM a2; SELECT count() FROM b; SELECT count() FROM b2; ${disks}"
    rm -rf "${dir}"
}

run_case created_before_26_7 ''
run_case created_on_26_8 "${add_s3_marker}"
run_case created_on_26_11 "${add_both_markers}"
# The value may also be a quoted identifier.
run_case created_on_26_11_identifier "${add_both_markers}" '`from_env T`'

# A table detached permanently is not loaded at startup, so its disk is not registered when it is attached again. The
# short `ATTACH` must still find the disk by the definition stored by the older version, not create an empty one.
# `local_blob_storage` keeps its metadata in a directory named after the disk.
export T2=local_blob_storage
dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_permanently_detached"
rm -rf "${dir}"
run() { ${CLICKHOUSE_LOCAL} --path "${dir}/data" --dynamic_disk_allow_from_env=1 -mn -q "$1" -- --custom_local_disks_base_directory="${dir}/" 2>&1 | grep -oE '^[0-9]+$|\([A-Z_]+\)'; }
echo "--- permanently_detached"
run "CREATE DATABASE d ENGINE = Atomic; CREATE TABLE d.p (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = disk(type = 'from_env T2', path = '${dir}/blobs/'); INSERT INTO d.p VALUES (7)"
old_disk=$(ls "${dir}/data/disks" | grep __tmp_internal_)
sed -i "${add_both_markers}" "${dir}"/data/store/*/*/p.sql
# Load the definition once to learn the name an older version gave the disk, and move the metadata there.
run "SELECT 1" > /dev/null
legacy_disk=$(ls "${dir}/data/disks" | grep __tmp_internal_ | grep -v "${old_disk}")
rm -rf "${dir}/data/disks/${legacy_disk}"
mv "${dir}/data/disks/${old_disk}" "${dir}/data/disks/${legacy_disk}"
run "SELECT count() FROM d.p"
run "DETACH TABLE d.p PERMANENTLY"
echo "attach"
run "ATTACH TABLE d.p; INSERT INTO d.p VALUES (8); SELECT count() FROM d.p"
echo "after restart"
run "SELECT count() FROM d.p"
rm -rf "${dir}"

# A wrapper declared by an older version names the disk of the older form below it, while a newer table can have
# registered the disk of the newer form too. Declaring the same wrapper again must find the wrapper.
dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_mixed"
rm -rf "${dir}"
run() { ${CLICKHOUSE_LOCAL} --path "${dir}/data" --dynamic_disk_allow_from_env=1 -mn -q "$1" -- --custom_local_disks_base_directory="${dir}/" --custom_cached_disks_base_directory="${dir}/caches/" 2>&1 | grep -oE '^[0-9a-z_]+$|\([A-Z_]+\)'; }
leaf="disk(type = 'from_env T2', path = '${dir}/blobs/')"
wrapper="disk(name = 'wrapper_disk', type = cache, path = '${dir}/caches/w/', max_size = '1Gi', disk = ${leaf})"
echo "--- mixed"
run "CREATE TABLE w (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${wrapper}; CREATE TABLE l (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${leaf}"
sed -i "${add_both_markers}" "${dir}"/data/store/*/*/w.sql
run "CREATE TABLE w2 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${wrapper}; SELECT 'wrapper_ok'"
w2_definition=$(grep -o "disk = disk(.*)" "${dir}"/data/store/*/*/w2.sql)
[ -n "${w2_definition}" ] && [ "${w2_definition}" = "$(grep -o "disk = disk(.*)" "${dir}"/data/store/*/*/w.sql)" ] && echo "w2 same as w"
rm -rf "${dir}"
