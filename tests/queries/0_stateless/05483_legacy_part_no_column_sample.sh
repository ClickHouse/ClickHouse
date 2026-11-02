#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: builds two wide parts and runs four `clickhouse local` instances over them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# A part written before `columns_substreams.txt` existed has to have its streams enumerated per
# column, and that enumeration excludes the dynamic streams. Taking a column sample for it is work
# whose result is dropped, and only a type with dynamic subcolumns pays it - `Map` always does,
# `String` never - so the two arms below differ in exactly that and in nothing else.
#
# Asserted by counting opened files rather than by succeeding: the check passes either way, so a
# success-only test cannot tell the sample from its absence. The part is made legacy by removing
# that one file, which is not in `checksums.txt` and so leaves the part valid.

workdir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${workdir}"
mkdir -p "${workdir}/db" "${workdir}/backups"

cat > "${workdir}/config.xml" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${workdir}/backups</allowed_path>
    </backups>
</clickhouse>
EOF

local_query() {
    ${CLICKHOUSE_LOCAL} --path "${workdir}/db" --config-file "${workdir}/config.xml" \
        --print-profile-events --query "$1" 2>&1
}

# Counts the files opened by one query. A sample is taken per column, so the arms are compared by
# their difference, not against a constant.
opened_files() {
    local_query "$1" | sed -n 's/.*\] FileOpen: \([0-9]*\) .*/\1/p' | tail -1
}

columns_map=""; columns_str=""; values_map=""; values_str=""
for i in $(seq 0 39); do
    columns_map="${columns_map:+${columns_map}, }c${i} Map(String, String)"
    columns_str="${columns_str:+${columns_str}, }c${i} String"
    values_map="${values_map:+${values_map}, }map('k', toString(number))"
    values_str="${values_str:+${values_str}, }toString(number)"
done

local_query "
    CREATE TABLE t_map (id UInt64, ${columns_map}) ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
    CREATE TABLE t_str (id UInt64, ${columns_str}) ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
    INSERT INTO t_map SELECT number, ${values_map} FROM numbers(20000);
    INSERT INTO t_str SELECT number, ${values_str} FROM numbers(20000);
" > /dev/null

check_map_before=$(opened_files "CHECK TABLE t_map SETTINGS check_query_single_value_result = 1")
check_str_before=$(opened_files "CHECK TABLE t_str SETTINGS check_query_single_value_result = 1")
backup_map_before=$(opened_files "BACKUP TABLE t_map TO File('m_before') SETTINGS check_parts = 1")
backup_str_before=$(opened_files "BACKUP TABLE t_str TO File('s_before') SETTINGS check_parts = 1")

# Make both parts legacy. `columns_substreams.txt` carries no checksum, so the part stays valid.
find "${workdir}/db/store" -name 'columns_substreams.txt' -delete

check_map_after=$(opened_files "CHECK TABLE t_map SETTINGS check_query_single_value_result = 1")
check_str_after=$(opened_files "CHECK TABLE t_str SETTINGS check_query_single_value_result = 1")
backup_map_after=$(opened_files "BACKUP TABLE t_map TO File('m_after') SETTINGS check_parts = 1")
backup_str_after=$(opened_files "BACKUP TABLE t_str TO File('s_after') SETTINGS check_parts = 1")

# What the fallback costs each arm. Without the fix the `Map` arm opens about five files per column
# more than it did, and the `String` arm opens none: the bound is one per column, well inside that.
report() {
    local name=$1 map_before=$2 map_after=$3 str_before=$4 str_after=$5
    local excess=$(( (map_after - map_before) - (str_after - str_before) ))
    if [[ "${excess}" -lt 40 ]]; then
        echo -e "${name}\t1"
    else
        echo -e "${name}\t0 (opened ${excess} files for 40 Map columns)"
    fi
}

report "check table opens no sample" "${check_map_before}" "${check_map_after}" "${check_str_before}" "${check_str_after}"
report "backup opens no sample" "${backup_map_before}" "${backup_map_after}" "${backup_str_before}" "${backup_str_after}"

rm -rf "${workdir}"
