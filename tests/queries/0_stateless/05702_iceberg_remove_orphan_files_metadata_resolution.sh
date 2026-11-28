#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows

# `remove_orphan_files` and INSERT root reachability at the authoritative current metadata file, never at
# a pinned, hinted or foreign-named one that would hide committed snapshots and get their files deleted.
# When the metadata directory does not say which file is current, both refuse without touching storage.
# Which file is current is decided by the resolver, not by the storage, so local storage covers it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./iceberg_orphan_files.lib
. "$CUR_DIR"/iceberg_orphan_files.lib

uuid()
{
    tr - _ < /proc/sys/kernel/random/uuid
}

# Highest N among the v<N>.metadata.json files.
newest_version()
{
    find "${META}" -maxdepth 1 -name 'v*.metadata.json' -printf '%f\n' \
        | sed -nE 's/^v([0-9]+)\.metadata\.json$/\1/p' | sort -n | tail -1
}

# Metadata files the server parses as version $1: v<N>.metadata.json, v<N>-<uuid>.metadata.json, <N>-<uuid>.metadata.json.
files_with_version()
{
    find "${META}" -maxdepth 1 -name '*.metadata.json' -printf '%f\n' | sort \
        | while read -r name; do
            v=$(sed -nE 's/^v?([0-9]+)[.-].*/\1/p' <<<"${name}")
            [ -n "${v}" ] && [ $((10#${v})) -eq "$1" ] && echo "${name}"
        done
}

# Committed data files: the parquet files under data/ except planted orphans.
data_files()
{
    list_files | grep '^data/.*\.parquet$' | grep -v orphan
}

# A hint that names an older version than the newest committed file is what a failed hint PUT, or a commit
# by another engine, leaves behind.
write_hint()
{
    printf '%s' "$1" > "${META}/version-hint.text"
    [ "$(cat "${META}/version-hint.text")" = "$1" ] || echo "hint rewrite to $1 did not take effect"
}

last_updated_ms()
{
    jq '."last-updated-ms"' "${META}/$1"
}

# A stale file carries a newer timestamp when it was written back after its commit, e.g. restored from a backup.
set_last_updated_ms()
{
    jq --argjson ms "$2" '."last-updated-ms" = $ms' "${META}/$1" > "${META}/$1.tmp" && mv "${META}/$1.tmp" "${META}/$1"
}

# Re-spells the v<N> chain the way iceberg-java names commits; RENAMED[N] holds the new names.
declare -a RENAMED
rename_chain_to_uuid()
{
    local v
    for v in $(seq "$1"); do
        RENAMED[v]=$(printf '%05d-%s.metadata.json' "$v" "$(uuid)")
        mv "${META}/v${v}.metadata.json" "${META}/${RENAMED[v]}"
    done
}

# Runs remove_orphan_files at now and prints its deleted_data_files_count, or the refusal matching $1.
expect_cleanup()
{
    run_expect "$1" -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '$(now_ts)')"
}

# Runs a query; prints "refused: <pattern>" when it fails with an error containing $1, the
# deleted_data_files_count when it succeeds, and the raw output otherwise.
run_expect()
{
    local pattern=$1 out
    shift
    if out=$($CH "$@" 2>&1); then
        awk -F'\t' '$1 == "deleted_data_files_count" { print "deleted data files: " $2 }' <<<"${out}"
        echo "ok"
    elif [ -n "${pattern}" ] && grep -qF "${pattern}" <<<"${out}"; then
        echo "refused: ${pattern}"
    else
        echo "${out}"
    fi
}

same()
{
    if [ "$1" = "$2" ]; then echo "$3 unchanged"; else echo "$3 changed"; diff <(echo "$1") <(echo "$2"); fi
}

UNDECLARED="nothing declares which of the two schemes"
RANKING="ranking selects"
EITHER_SIDE="may rank either side of the declared one"

echo "--- a table pinned to older metadata still roots cleanup at the newest"
populate local pinned 2
pinned_path="metadata/v$(newest_version).metadata.json"
$CH -q "INSERT INTO ${TABLE} VALUES (3)"
before=$(data_files)
echo "data files: $(wc -l <<<"${before}")"
create ", iceberg_metadata_file_path = '${pinned_path}'"
$CH -q "SELECT count() FROM ${TABLE}"
expect_cleanup ""
same "${before}" "$(data_files)" "data files"
create ""
data

echo "--- a stale version hint keeps the committed snapshot"
populate local stale_hint 3 ", iceberg_use_version_hint = true"
newest=$(newest_version)
echo "newest: ${newest}"
exists metadata/version-hint.text
# A planted orphan shows the cleanup ran: a binary that stops deleting passes the survival checks.
add_orphan data/orphan-stale-hint.parquet
# A foreign-named metadata file and the temporary file of an interrupted commit are not candidates.
cp "${META}/v${newest}.metadata.json" "${META}/00099-$(uuid).metadata.json"
add_orphan "metadata/$(uuid).metadata.json"
write_hint $((newest - 1))
before=$(data_files)
expect_cleanup ""
same "${before}" "$(data_files)" "data files"
exists "metadata/v${newest}.metadata.json"
exists data/orphan-stale-hint.parquet
write_hint "${newest}"
data

# A writer that takes its next version from a lagging hint aims at a version already present, so every
# conditional commit is refused until the retries run out.
echo "--- INSERT under a stale version hint commits a newer version"
populate local insert_stale_hint 3 ", iceberg_use_version_hint = true"
newest=$(newest_version)
echo "newest: ${newest}"
write_hint $((newest - 1))
run_expect "" -q "INSERT INTO ${TABLE} VALUES (4)"
echo "newest after INSERT: $(newest_version)"
data

# <N>-<uuid>.metadata.json is committed through a pointer this table does not read, so its N is not
# comparable with this table's v<N>. The hint names the scheme, with or without a directory part.
echo "--- a foreign-scheme metadata file does not become the root"
populate local foreign_scheme 3 ", iceberg_use_version_hint = true"
newest=$(newest_version)
echo "newest: ${newest}"
for hint in "${newest}" "metadata/v${newest}.metadata.json"; do
    echo "hint: ${hint}"
    write_hint "${hint}"
    # An earlier state of this table under a higher foreign number: rooting there loses the newest snapshot.
    foreign=$(printf '%05d-%s.metadata.json' $((newest + 1)) "$(uuid)")
    cp "${META}/v$((newest - 1)).metadata.json" "${META}/${foreign}"
    [ "$(files_with_version $((newest + 1)))" = "${foreign}" ] || echo "foreign file does not outrank the committed one"
    orphan="orphan-foreign-scheme-$(uuid).parquet"
    add_orphan "data/${orphan}"
    before=$(data_files)
    expect_cleanup ""
    same "${before}" "$(data_files)" "data files"
    exists "metadata/v${newest}.metadata.json"
    [ -f "${META}/${foreign}" ] && echo "foreign file kept" || echo "foreign file removed"
    [ -f "${DIR}/data/${orphan}" ] && echo "orphan kept" || echo "orphan removed"
    data
done

# With nothing declaring the scheme the two numberings cannot be ranked, so the command refuses.
echo "--- an undeclared scheme is refused by cleanup and by INSERT"
populate local undeclared 3
newest=$(newest_version)
echo "newest: ${newest}"
exists metadata/version-hint.text
foreign=$(printf '%05d-%s.metadata.json' $((newest + 1)) "$(uuid)")
cp "${META}/v$((newest - 1)).metadata.json" "${META}/${foreign}"
[ "$(files_with_version $((newest + 1)))" = "${foreign}" ] || echo "foreign file does not outrank the committed one"
before=$(list_files)
expect_cleanup "${UNDECLARED}"
same "${before}" "$(list_files)" "files"
run_expect "${UNDECLARED}" -q "INSERT INTO ${TABLE} VALUES (4)"
same "${before}" "$(list_files)" "files"
# Control: unambiguous once the foreign file is gone, so a binary that refuses every INSERT fails.
rm "${META}/${foreign}"
run_expect "" -q "INSERT INTO ${TABLE} VALUES (4)"
echo "newest after INSERT: $(newest_version)"
data

# Under the last-updated-ms policy two different versions with one timestamp cannot be ordered.
echo "--- equal last-updated-ms is refused"
populate local ms_tie 3 ", iceberg_recent_metadata_file_by_last_updated_ms_field = true"
# Control: the policy alone does not stop the cleanup.
add_orphan data/orphan-before-tie.parquet
expect_cleanup ""
newest=$(newest_version)
cp "${META}/v${newest}.metadata.json" "${META}/v$((newest + 1)).metadata.json"
# One file per version, so a check keyed on the version number would see nothing wrong.
files_with_version "${newest}"
files_with_version $((newest + 1))
add_orphan data/orphan-ms-tie.parquet
before=$(list_files)
expect_cleanup "rank equal by their last-updated-ms field"
same "${before}" "$(list_files)" "files"
data

# A timestamp says when a file was written, not which file the table committed, so the ranking can land on
# a file the pointer contradicts: one below the declared version, or a second file spelling that version.
echo "--- last-updated-ms ranking against the pointer is refused"
populate local ms_pointer 3 ", iceberg_use_version_hint = true, iceberg_recent_metadata_file_by_last_updated_ms_field = true"
newest=$(newest_version)
echo "newest: ${newest}"
[ "$(cat "${META}/version-hint.text")" = "${newest}" ] || echo "hint does not name the committed version"
declared_ms=$(last_updated_ms "v${newest}.metadata.json")
add_orphan data/orphan-ms-pointer.parquet
# v2 is the state after the first insert, restored under a name no query has read, timestamped later.
cp "${META}/v2.metadata.json" "${META}/v0.metadata.json"
set_last_updated_ms v0.metadata.json $((declared_ms + 1000))
before=$(list_files)
expect_cleanup "${RANKING}"
same "${before}" "$(list_files)" "files"
rm "${META}/v0.metadata.json"
# The twin holds the declared version with different content, which a version comparison cannot see.
rename_chain_to_uuid "${newest}"
write_hint "${RENAMED[newest]}"
twin=$(printf '%05d-%s.metadata.json' "${newest}" "$(uuid)")
cp "${META}/${RENAMED[2]}" "${META}/${twin}"
set_last_updated_ms "${twin}" $((declared_ms + 1000))
[ "$(files_with_version "${newest}")" = "$(printf '%s\n' "${RENAMED[newest]}" "${twin}" | sort)" ] || echo "no second file on the declared version"
before=$(list_files)
expect_cleanup "${RANKING}"
same "${before}" "$(list_files)" "files"
# Control: with the twin gone the pointer and the ranking agree.
rm "${META}/${twin}"
expect_cleanup ""
exists data/orphan-ms-pointer.parquet
data

# On the all-uuid layout the writer chooses the prefix, so a higher one can name an abandoned write.
echo "--- a uuid-named file outranking the pointer is refused"
populate local uuid_pointer 3 ", iceberg_use_version_hint = true"
newest=$(newest_version)
echo "newest: ${newest}"
rename_chain_to_uuid "${newest}"
write_hint "${RENAMED[newest]}"
stray=$(printf '%05d-%s.metadata.json' $((newest + 2)) "$(uuid)")
cp "${META}/${RENAMED[2]}" "${META}/${stray}"
[ "$(files_with_version $((newest + 2)))" = "${stray}" ] || echo "stray file does not outrank the declared one"
add_orphan data/orphan-uuid-ptr.parquet
before=$(list_files)
expect_cleanup "${RANKING}"
same "${before}" "$(list_files)" "files"
# Control: a binary that refuses every uuid-named pointer fails here.
rm "${META}/${stray}"
expect_cleanup ""
exists data/orphan-uuid-ptr.parquet
data

# No commit advances iceberg_metadata_file_path, so a uuid-named pin beside the v<N> chain hides the
# table's own newest snapshot from a write.
echo "--- INSERT under a uuid-named pin beside the v<N> chain is refused"
populate local uuid_pin 3
newest=$(newest_version)
declared=$(printf '%05d-%s.metadata.json' "${newest}" "$(uuid)")
cp "${META}/v${newest}.metadata.json" "${META}/${declared}"
create ", iceberg_metadata_file_path = 'metadata/${declared}'"
$CH -q "SELECT count() FROM ${TABLE}"
before=$(list_files)
run_expect "${EITHER_SIDE}" -q "INSERT INTO ${TABLE} VALUES (4)"
same "${before}" "$(list_files)" "files"
# Control: without the uuid-named file the same table commits and reads.
rm "${META}/${declared}"
create ""
run_expect "" -q "INSERT INTO ${TABLE} VALUES (4)"
data

# An old pin kept for time-travel reads must not hide a current file another engine committed uuid-named.
echo "--- a pin superseded in its own scheme is refused"
populate local vn_pin 4 ", iceberg_use_version_hint = true"
newest=$(newest_version)
current=$(printf '%05d-%s.metadata.json' "${newest}" "$(uuid)")
mv "${META}/v${newest}.metadata.json" "${META}/${current}"
write_hint "${current}"
create ", iceberg_use_version_hint = true, iceberg_metadata_file_path = 'metadata/v$((newest - 3)).metadata.json'"
before=$(list_files)
expect_cleanup "${EITHER_SIDE}"
same "${before}" "$(list_files)" "files"
# With the hint off the pin is the only pointer, and a path no commit advances cannot declare the scheme.
create ", iceberg_metadata_file_path = 'metadata/v$((newest - 3)).metadata.json'"
expect_cleanup "${UNDECLARED}"
same "${before}" "$(list_files)" "files"
# Control: without the pin and the leftover v<N> history the layout is unambiguous.
create ", iceberg_use_version_hint = true"
for v in $(seq $((newest - 1))); do rm "${META}/v${v}.metadata.json"; done
add_orphan data/orphan-vn-pin.parquet
expect_cleanup ""
[ -f "${META}/${current}" ] && echo "current file kept" || echo "current file removed"
data

$CH -q "DROP TABLE ${TABLE}"
