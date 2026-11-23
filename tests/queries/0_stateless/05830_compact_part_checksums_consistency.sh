#!/usr/bin/env bash
# MergeTreeDataPartCompact::doCheckConsistency rejects a compact part whose checksums.txt does not
# list count.txt, the marks file or data.bin, a part whose count.txt is empty on disk, and a part
# whose marks file has no data file beside it. The marks and data entries are only required from a
# metadata caller: BACKUP (`check_parts`, on). Each case keeps the files the other checks read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(cd "${CLICKHOUSE_TMP}" && pwd)/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

CONFIG="${WORK_DIR}/config.xml"
printf '<clickhouse><backups><allowed_path>%s</allowed_path></backups></clickhouse>' \
    "${WORK_DIR}/backups" > "${CONFIG}"

# Only the error code name is asserted: the messages carry paths, file names and sizes.
code()
{
    grep -o -a -m 1 -F -e '(NO_FILE_IN_DATA_PART)' -e '(BAD_SIZE_OF_FILE_IN_DATA_PART)' \
        -e 'BACKUP_CREATED' || echo 'no error'
}

# Compact part, with checksums.txt a file on disk rather than an entry inside data.packed.
new_part()
{
    ${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/$1" -m -q "
        CREATE TABLE part_check (k UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;
        INSERT INTO part_check VALUES (1, 'a'), (2, 'b');"
    find "${WORK_DIR}/$1" -type d -name 'all_*' | head -n 1
}

# Legacy checksums (version 2) are plain text; loading compares the recorded sizes, not the hashes.
keep_only()
{
    local dir=$1; shift
    {   printf 'checksums format version: 2\n%s files:\n' "$#"
        for f in "$@"; do
            printf '%s\n\tsize: %s\n\thash: 0 0\n\tcompressed: 0\n' "$f" "$(stat -c%s "$dir/$f")"
        done
    } > "$dir/checksums.txt"
}

# A fresh process per case: the checksums are read when the part is loaded.
backup()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/$1" \
        -q "BACKUP TABLE part_check TO File('${WORK_DIR}/backups/$1') FORMAT TSV" 2>&1 | code
}

# `data.bin` stays listed, so the data file entry check passes and the missing marks entry is the
# only reason left: listing the marks file too makes the same BACKUP succeed (third case below).
part=$(new_part no_marks)
marks=$(cd "$part" && ls -1 data.*mrk* | head -n 1)
keep_only "$part" count.txt data.bin
echo -n 'no marks file checksum: '
backup no_marks
# The part itself is readable: only the check that wants the whole part metadata rejects it.
echo -n 'rows still readable: '
${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/no_marks" -q "SELECT count() FROM part_check"

part=$(new_part no_data)
keep_only "$part" count.txt "${marks}"
echo -n 'no data file checksum: '
backup no_data

part=$(new_part complete)
mapfile -t files < <(cd "$part" && ls -1 | grep -v -x 'checksums.txt')
keep_only "$part" "${files[@]}"
echo -n 'every file listed: '
backup complete

# The deprecated syntax gives format version 0, where checkConsistencyBase checks neither the
# count.txt entry of the checksums nor the count.txt file, so the compact part's own checks reject
# these parts. Such a table takes no SETTINGS clause, but two rows make a compact part anyway.
old_part()
{
    ${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/$1" -m -q "
        SET allow_deprecated_syntax_for_merge_tree = 1;
        CREATE TABLE part_check (d Date, k UInt64) ENGINE = MergeTree(d, k, 8192);
        INSERT INTO part_check VALUES ('2020-01-01', 1), ('2020-01-02', 2);
        ALTER TABLE part_check DETACH PARTITION 202001;"
    find "${WORK_DIR}/$1" -type d -path '*/detached/*' | head -n 1
}

attach()
{
    ${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/$1" \
        -q "ALTER TABLE part_check ATTACH PART '$2'" 2>&1 | code
}

part=$(old_part no_count)
keep_only "$part" primary.cidx
echo -n 'no count.txt checksum: '
attach no_count "$(basename "$part")"

# Checksums that vouch for no file cannot describe a file torn by a crash, so count.txt and the
# marks are checked on disk too. Empty marks make the row count come from the marks, so an empty
# count.txt survives; `data.bin` is gone so only the count.txt check can report an empty file.
torn_part()
{
    part=$(old_part "$1")
    keep_only "$part"
    [ -z "$2" ] || : > "$part/count.txt"
    : > "$part/$(cd "$part" && ls -1 data.*mrk* | head -n 1)"
    rm -f "$part/data.bin"
    attach "$1" "$(basename "$part")"
}

echo -n 'empty count.txt: '
torn_part empty_count empty
# The same part with count.txt kept: the next check reports the data file under another code.
echo -n 'no data file beside marks: '
torn_part no_data_beside_marks

rm -rf "${WORK_DIR}"
