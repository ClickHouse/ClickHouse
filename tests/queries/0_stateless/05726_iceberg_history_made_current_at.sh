#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# made_current_at of system.iceberg_history: the last snapshot-log entry of a snapshot wins, and a snapshot
# absent from snapshot-log (trimmed by expire_snapshots, or the whole log missing) falls back to its own
# timestamp-ms instead of the epoch. Snapshot-log variants a ClickHouse writer never produces (log time differs
# from commit time, repeated snapshot id after a rollback) are handcrafted as new metadata versions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./iceberg.lib
. "$CUR_DIR"/iceberg.lib

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_expire_snapshots=1"

create()
{
    ${CH} -q "CREATE TABLE $1 (x Int) ENGINE = IcebergLocal('${ROOT}/$1/') SETTINGS iceberg_format_version = 2"
}

# Prints each snapshot of table $1 in commit order: operation, is_current_ancestor, and made_current_at minus
# the snapshot's own timestamp-ms in the newest metadata.
history()
{
    local meta
    meta=$(iceberg_latest_metadata "${ROOT}/$1/metadata")
    ${CH} --param_ids="$(jq -c '[.snapshots[]."snapshot-id"]' "${meta}")" \
          --param_seqs="$(jq -c '[.snapshots[]."sequence-number"]' "${meta}")" \
          --param_ts="$(jq -c '[.snapshots[]."timestamp-ms"]' "${meta}")" \
          -q "
        WITH mapFromArrays({ids:Array(UInt64)}, {seqs:Array(UInt64)}) AS seq,
             mapFromArrays({ids:Array(UInt64)}, {ts:Array(Int64)}) AS ts
        SELECT operation, is_current_ancestor, toUnixTimestamp64Milli(made_current_at) - ts[snapshot_id]
        FROM system.iceberg_history WHERE database = currentDatabase() AND table = '$1'
        ORDER BY seq[snapshot_id]"
}

# Prints the number of snapshots of table $1 and how many of them snapshot-log does not mention.
log_coverage()
{
    jq -c '[(.snapshots | length), ([.snapshots[]."snapshot-id"] - [(."snapshot-log" // [])[]."snapshot-id"] | length)]' \
        "$(iceberg_latest_metadata "${ROOT}/$1/metadata")"
}

# Expiring a middle snapshot trims snapshot-log so a retained snapshot drops out of it; its made_current_at
# stays the same (ClickHouse writes equal log and commit timestamps, so the fallback is lossless).
echo "--- stable after expire"
create t_expire
${CH} -q "INSERT INTO t_expire VALUES (1), (2), (3)"
${CH} -q "ALTER TABLE t_expire DELETE WHERE 1 SETTINGS mutations_sync = 2"
${CH} -q "INSERT INTO t_expire VALUES (4), (5)"
history t_expire
log_coverage t_expire
overwrite=$(${CH} -q "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't_expire' AND operation = 'OVERWRITE'")
${CH} -q "ALTER TABLE t_expire EXECUTE expire_snapshots(snapshot_ids = [${overwrite}])" > /dev/null
history t_expire
log_coverage t_expire

# snapshot-log is optional: the table is still reported, with each snapshot's own timestamp-ms.
echo "--- without snapshot-log"
create t_no_log
${CH} -q "INSERT INTO t_no_log VALUES (1), (2)"
log_coverage t_no_log
iceberg_edit_metadata "${ROOT}/t_no_log/metadata" 'del(."snapshot-log")' > /dev/null
log_coverage t_no_log
history t_no_log

# Creates table $1 with two snapshots and prints the commit time of the newer one. The INSERTs land milliseconds
# apart, so the edits below lay the older snapshot's times out back from the newer commit.
two_snapshots()
{
    create "$1"
    ${CH} -q "INSERT INTO $1 VALUES (1)"
    ${CH} -q "INSERT INTO $1 VALUES (2)"
    jq '.snapshots | max_by(."sequence-number") | ."timestamp-ms"' "$(iceberg_latest_metadata "${ROOT}/$1/metadata")"
}

# Log time and commit time a second apart; trimming the log entry moves made_current_at to the commit time,
# not the epoch and not the old log time.
echo "--- dropped log entry falls back to commit time"
newer=$(two_snapshots t_log_time_differs)
iceberg_edit_metadata "${ROOT}/t_log_time_differs/metadata" '
    .snapshots |= sort_by(."sequence-number")
    | .snapshots[0]."timestamp-ms" = $newer - 2000
    | ."snapshot-log" = [
        {"snapshot-id": .snapshots[0]."snapshot-id", "timestamp-ms": ($newer - 1000)},
        {"snapshot-id": .snapshots[1]."snapshot-id", "timestamp-ms": $newer}]' --argjson newer "${newer}" > /dev/null
history t_log_time_differs
iceberg_edit_metadata "${ROOT}/t_log_time_differs/metadata" 'del(."snapshot-log"[0])' > /dev/null
history t_log_time_differs

# A rollback leaves the same snapshot id in snapshot-log twice; the last entry is when it became current.
# A snapshot cannot become current before it is committed, so its commit time moves back too, and the rest
# of the metadata follows the rolled-back state.
echo "--- repeated snapshot id uses the last log entry"
newer=$(two_snapshots t_repeated_log_id)
iceberg_edit_metadata "${ROOT}/t_repeated_log_id/metadata" '
    .snapshots |= sort_by(."sequence-number")
    | .snapshots[0]."timestamp-ms" = $newer - 2000
    | ."snapshot-log" = [
        {"snapshot-id": .snapshots[0]."snapshot-id", "timestamp-ms": ($newer - 2000)},
        {"snapshot-id": .snapshots[1]."snapshot-id", "timestamp-ms": $newer},
        {"snapshot-id": .snapshots[0]."snapshot-id", "timestamp-ms": ($newer + 1000)}]
    | ."current-snapshot-id" = .snapshots[0]."snapshot-id"
    | .refs.main."snapshot-id" = .snapshots[0]."snapshot-id"
    | ."last-updated-ms" = $newer + 1000' --argjson newer "${newer}" > /dev/null
history t_repeated_log_id

${CH} -q "DROP TABLE t_expire; DROP TABLE t_no_log; DROP TABLE t_log_time_differs; DROP TABLE t_repeated_log_id"
