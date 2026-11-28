#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: builds 120-column wide parts and runs four `clickhouse local` instances over them.

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

# The two arms of one phase are measured in the same process, and neither is the first query in it.
#
# `FileOpen` counts the whole process, so a measurement carries whatever the process happened to do
# besides the query: starting up, touching the data directory for the first time, creating the
# system tables a `BACKUP` writes on its first run. Those costs cancel out of the difference between
# the arms only when both arms pay them on the same side of it. Measuring each arm in a process of
# its own did not do that, and the backup arm failed 4 of 50 runs of the flaky check, reporting an
# excess of 420 against a bound of 300 - noise the size of the signal.
#
# So a phase is one process: a warm-up query that is not measured, then the `Map` arm, then the
# `String` arm, with the counter read between them. The warm-up pays the once-per-process and
# once-per-directory costs, and the two deltas that follow differ by the column type alone.
#
# A measured query that fails must not be read as a measurement: its status does not reach `set -e`
# through the assignment below, and the readings printed before it would still subtract into a
# plausible-looking count. The status and the shape of the output are both checked here, and the
# diagnostic is kept rather than discarded so a failure says what went wrong.
phase() {
    local dir=$1 warmup=$2 query_map=$3 query_str=$4 out r0 r1 r2
    if ! out=$(${CLICKHOUSE_LOCAL} --path "${dir}/db" --config-file "${dir}/config.xml" --multiquery --query "
        ${warmup} FORMAT Null;
        SELECT value FROM system.events WHERE event = 'FileOpen';
        ${query_map} FORMAT Null;
        SELECT value FROM system.events WHERE event = 'FileOpen';
        ${query_str} FORMAT Null;
        SELECT value FROM system.events WHERE event = 'FileOpen';" 2>"${root}/stderr")
    then
        echo "measured queries failed: ${query_map} / ${query_str}" >&2
        cat "${root}/stderr" >&2
        return 1
    fi
    r0=$(echo "${out}" | sed -n 1p)
    r1=$(echo "${out}" | sed -n 2p)
    r2=$(echo "${out}" | sed -n 3p)
    if ! [[ "${r0}" =~ ^[0-9]+$ && "${r1}" =~ ^[0-9]+$ && "${r2}" =~ ^[0-9]+$ ]]; then
        echo "no FileOpen counter at all three points of: ${query_map} / ${query_str}" >&2
        echo "${out}" >&2
        return 1
    fi
    echo "$(( r1 - r0 )) $(( r2 - r1 ))"
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
# opens none more, so the two differ by 600 of them here; with it they differ by none.
measure() {
    local name=$1 dir=$2 warm_before=$3 query_map=$4 query_str=$5 warm_after=$6 query_map2=$7 query_str2=$8
    setup "${dir}"
    local before after mb sb ma sa legacy
    before=$(phase "${dir}" "${warm_before}" "${query_map}" "${query_str}") || exit 1
    # `columns_substreams.txt` carries no checksum, so the part stays valid without it. Removing
    # nothing would leave both phases measuring the same part and the difference near zero, which
    # reads as the fix working, so require that the parts were made legacy.
    legacy=$(find "${dir}/db/store" -name 'columns_substreams.txt' -print -delete | wc -l)
    if [[ "${legacy}" -eq 0 ]]; then
        echo "no columns_substreams.txt under ${dir}/db/store" >&2
        exit 1
    fi
    after=$(phase "${dir}" "${warm_after}" "${query_map2}" "${query_str2}") || exit 1
    read -r mb sb <<<"${before}"
    read -r ma sa <<<"${after}"
    # Grouped per phase: each subtraction is between two queries of one process.
    local excess=$(( (ma - sa) - (mb - sb) ))
    if [[ "${excess}" -lt 300 ]]; then
        echo -e "${name}\t1"
    else
        # The four readings go to stderr, which the harness keeps on a failure. The excess alone does
        # not say whether an arm moved or the two phases disagree, and the run that reports it is not
        # one that can be repeated afterwards.
        echo "${name}: map ${mb} -> ${ma}, str ${sb} -> ${sa}" >&2
        echo -e "${name}\t0 (opened ${excess} files for 120 Map columns)"
    fi
}

measure "check table opens no sample" "${root}/check" \
    "CHECK TABLE t_str" "CHECK TABLE t_map" "CHECK TABLE t_str" \
    "CHECK TABLE t_str" "CHECK TABLE t_map" "CHECK TABLE t_str"
measure "backup opens no sample" "${root}/backup" \
    "BACKUP TABLE t_str TO File('warm_b')" "BACKUP TABLE t_map TO File('mb')" "BACKUP TABLE t_str TO File('sb')" \
    "BACKUP TABLE t_str TO File('warm_a')" "BACKUP TABLE t_map TO File('ma')" "BACKUP TABLE t_str TO File('sa')"

rm -rf "${root}"
