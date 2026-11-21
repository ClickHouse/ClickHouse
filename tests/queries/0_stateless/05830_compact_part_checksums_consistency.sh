#!/usr/bin/env bash
# MergeTreeDataPartCompact::doCheckConsistency rejects a compact part whose checksums.txt does not
# list count.txt, the marks file or data.bin, and a part whose count.txt is empty on disk. The marks
# and data file entries are only required from a caller that asks for the part metadata, which is
# what BACKUP does for every part it collects (backup setting `check_parts`, on by default).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(cd "${CLICKHOUSE_TMP}" && pwd)/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

CONFIG="${WORK_DIR}/config.xml"
cat > "${CONFIG}" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/backups</allowed_path>
    </backups>
</clickhouse>
EOF

# Only the error code name is asserted: the messages carry part paths, file names and sizes.
code()
{
    grep -o -a -m 1 -F -e '(NO_FILE_IN_DATA_PART)' -e '(BAD_SIZE_OF_FILE_IN_DATA_PART)' \
        -e 'BACKUP_CREATED' || echo 'no error'
}

# `min_bytes_for_wide_part` keeps the part compact and `min_bytes_for_full_part_storage = 0` keeps
# checksums.txt a file on disk instead of an entry inside data.packed.
new_part()
{
    ${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/$1" -m -q "
        CREATE TABLE part_check (k UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;
        INSERT INTO part_check VALUES (1, 'a'), (2, 'b');"
    find "${WORK_DIR}/$1" -type d -name 'all_*' | head -n 1
}

# Legacy checksums (format version 2) are plain text, so an arbitrary file list can be written by
# hand. Loading a part compares the recorded sizes only, so the hashes stay at zero.
keep_only()
{
    local dir=$1
    shift
    {
        printf 'checksums format version: 2\n%s files:\n' "$#"
        for f in "$@"; do
            printf '%s\n\tsize: %s\n\thash: 0 0\n\tcompressed: 0\n' "$f" "$(stat -c%s "$dir/$f")"
        done
    } > "$dir/checksums.txt"
}

# A fresh process per case: the checksums are read when the part is loaded, so the rewritten file
# has to be in place before the process that validates it starts.
backup()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/$1" \
        -q "BACKUP TABLE part_check TO File('${WORK_DIR}/backups/$1') FORMAT TSV" 2>&1 | code
}

part=$(new_part no_marks)
marks=$(cd "$part" && ls -1 data.*mrk* | head -n 1)
keep_only "$part" count.txt
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

# Checksums regenerated from the files on disk describe a file torn by a crash as it is, so count.txt
# is checked on disk as well in that case. An empty marks file makes the row count come from the
# marks, so the empty count.txt is not read before the consistency check.
new_part empty_count > /dev/null
${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/empty_count" -q "ALTER TABLE part_check DETACH PARTITION tuple()"
part=$(find "${WORK_DIR}/empty_count" -type d -path '*/detached/all_*' | head -n 1)
rm "$part/checksums.txt"
: > "$part/count.txt"
: > "$part/$(cd "$part" && ls -1 data.*mrk* | head -n 1)"
echo -n 'empty count.txt: '
${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/empty_count" \
    -q "ALTER TABLE part_check ATTACH PART '$(basename "$part")'" 2>&1 | code

# A table created with the deprecated syntax has format version 0, where the count.txt entry of the
# checksums is not checked by IMergeTreeDataPart::checkConsistencyBase, so the check of the compact
# part is the one that rejects the part. Such a table takes no SETTINGS clause; two rows stay far
# below the default `min_bytes_for_wide_part`, so the part is compact anyway.
${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/no_count" -m -q "
    SET allow_deprecated_syntax_for_merge_tree = 1;
    CREATE TABLE part_check (d Date, k UInt64) ENGINE = MergeTree(d, k, 8192);
    INSERT INTO part_check VALUES ('2020-01-01', 1), ('2020-01-02', 2);
    ALTER TABLE part_check DETACH PARTITION 202001;"
part=$(find "${WORK_DIR}/no_count" -type d -path '*/detached/*' | head -n 1)
keep_only "$part" primary.cidx
echo -n 'no count.txt checksum: '
${CLICKHOUSE_LOCAL} --path "${WORK_DIR}/no_count" \
    -q "ALTER TABLE part_check ATTACH PART '$(basename "$part")'" 2>&1 | code

rm -rf "${WORK_DIR}"
