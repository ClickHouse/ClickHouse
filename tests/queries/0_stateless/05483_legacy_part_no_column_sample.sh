#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: builds 120-column wide parts and runs eight `clickhouse local` instances over them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# A part written before `columns_substreams.txt` existed has its streams enumerated per column, and
# that enumeration excludes the dynamic streams. Taking a column sample for it is work whose result
# is dropped, and only a type with dynamic subcolumns pays it - `Map` always does, `String` never -
# so the two arms differ in exactly that and in nothing else.
#
# Asserted by counting opened files, not by succeeding: the check passes either way, so a
# success-only test cannot tell the sample from its absence. The part is made legacy by removing
# that one file, which carries no checksum and so leaves the part valid.
#
# `CHECK TABLE` and `BACKUP` get a directory each. A backup leaves system tables behind in the one
# it runs in, which moves the counts of anything measured there afterwards.

root="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${root}"
mkdir -p "${root}"

columns_map=""; columns_str=""; values_map=""; values_str=""
for i in $(seq 0 119); do
    columns_map="${columns_map:+${columns_map}, }c${i} Map(String, String)"
    columns_str="${columns_str:+${columns_str}, }c${i} String"
    values_map="${values_map:+${values_map}, }map('k', toString(number))"
    values_str="${values_str:+${values_str}, }toString(number)"
done

# Files opened by one query, bracketed inside the process that runs it: `FileOpen` counts the whole
# process, and what a `clickhouse local` opens to start differs between runs by enough to swamp this.
#
# A measured query that fails must not be read as a measurement. Its status does not reach `set -e`
# through the assignment below, and the first `SELECT` has already printed by the time the query
# itself throws, so a single surviving line would make the subtraction a unary minus and yield a
# plausible-looking count. Both the status and the two counters are therefore checked here, and the
# diagnostic is kept rather than discarded so a failure says what went wrong.
opened_files() {
    local dir=$1 query=$2 out before after
    if ! out=$(${CLICKHOUSE_LOCAL} --path "${dir}/db" --config-file "${dir}/config.xml" --multiquery --query "
        SELECT value FROM system.events WHERE event = 'FileOpen';
        ${query} FORMAT Null;
        SELECT value FROM system.events WHERE event = 'FileOpen';" 2>"${root}/stderr")
    then
        echo "measured query failed: ${query}" >&2
        cat "${root}/stderr" >&2
        return 1
    fi
    before=$(echo "${out}" | sed -n 1p)
    after=$(echo "${out}" | sed -n 2p)
    if ! [[ "${before}" =~ ^[0-9]+$ && "${after}" =~ ^[0-9]+$ ]]; then
        echo "no FileOpen counter on both sides of: ${query}" >&2
        echo "${out}" >&2
        return 1
    fi
    echo $(( after - before ))
}

setup() {
    local dir=$1
    mkdir -p "${dir}/db" "${dir}/backups"
    cat > "${dir}/config.xml" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${dir}/backups</allowed_path>
    </backups>
</clickhouse>
EOF
    ${CLICKHOUSE_LOCAL} --path "${dir}/db" --config-file "${dir}/config.xml" --multiquery --query "
        CREATE TABLE t_map (id UInt64, ${columns_map}) ENGINE = MergeTree ORDER BY id
            SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
        CREATE TABLE t_str (id UInt64, ${columns_str}) ENGINE = MergeTree ORDER BY id
            SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
        INSERT INTO t_map SELECT number, ${values_map} FROM numbers(8000);
        INSERT INTO t_str SELECT number, ${values_str} FROM numbers(8000);" > /dev/null
}

# Without the fix the `Map` arm opens five files per column more than it did and the `String` arm
# opens none more, so the two differ by 600 of them here; with it they differ by none. The bound is
# set well above zero rather than near it: the counts come from four processes, and one of them
# occasionally reads a couple of hundred files fewer than its fellows.
measure() {
    local name=$1 dir=$2 query_map=$3 query_str=$4 query_map2=$5 query_str2=$6
    setup "${dir}"
    local mb sb ma sa legacy
    mb=$(opened_files "${dir}" "${query_map}") || exit 1
    sb=$(opened_files "${dir}" "${query_str}") || exit 1
    # `columns_substreams.txt` carries no checksum, so the part stays valid without it. Removing
    # nothing would leave both arms measuring the same part twice and the difference near zero, which
    # reads as the fix working, so require that the parts were made legacy.
    legacy=$(find "${dir}/db/store" -name 'columns_substreams.txt' -print -delete | wc -l)
    if [[ "${legacy}" -eq 0 ]]; then
        echo "no columns_substreams.txt under ${dir}/db/store" >&2
        exit 1
    fi
    ma=$(opened_files "${dir}" "${query_map2}") || exit 1
    sa=$(opened_files "${dir}" "${query_str2}") || exit 1
    local excess=$(( (ma - mb) - (sa - sb) ))
    if [[ "${excess}" -lt 300 ]]; then
        echo -e "${name}\t1"
    else
        echo -e "${name}\t0 (opened ${excess} files for 120 Map columns)"
    fi
}

measure "check table opens no sample" "${root}/check" \
    "CHECK TABLE t_map" "CHECK TABLE t_str" "CHECK TABLE t_map" "CHECK TABLE t_str"
measure "backup opens no sample" "${root}/backup" \
    "BACKUP TABLE t_map TO File('mb')" "BACKUP TABLE t_str TO File('sb')" \
    "BACKUP TABLE t_map TO File('ma')" "BACKUP TABLE t_str TO File('sa')"

rm -rf "${root}"
