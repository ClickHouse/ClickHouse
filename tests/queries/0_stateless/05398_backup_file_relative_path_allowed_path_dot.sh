#!/usr/bin/env bash
# A relative File() destination is resolved against the first backups.allowed_path entry and must stay
# allowed when that entry, or the data path a relative entry is resolved against, has `.` or `..` components.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Absolute, because the working directory changes below.
WORK_DIR=$(cd "${CLICKHOUSE_TMP}" && pwd)/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/sub"
cd "${WORK_DIR}" || exit 1

# label|allowed_path|--path
for arm in \
    "dot|${WORK_DIR}/./backups|${WORK_DIR}/data" \
    "dotdot|${WORK_DIR}/sub/../backups|${WORK_DIR}/data" \
    "relative|../backups|${WORK_DIR}/data" \
    "relative_data_path|../backups|./data"
do
    IFS='|' read -r label allowed data <<< "${arm}"
    cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${allowed}</allowed_path>
    </backups>
</clickhouse>
EOF
    rm -rf "${WORK_DIR}/data" "${WORK_DIR}/backups"
    echo "${label}"
    ${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/config.xml" --path "${data}" -m -q "
        CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
        INSERT INTO t SELECT number FROM numbers(10);
        BACKUP TABLE t TO File('b1') FORMAT Null;
        RESTORE TABLE t AS t2 FROM File('b1') FORMAT Null;
        SELECT count(), sum(x) FROM t2;
        BACKUP TABLE t TO File('../backups/b2') FORMAT Null;
    "
    ls "${WORK_DIR}/backups"
    ${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/config.xml" --path "${data}" \
        -q "BACKUP TABLE t TO File('../escape') FORMAT Null" 2>&1 | grep -o -m1 BAD_ARGUMENTS
done

# A relative path that leaves the first entry must not be admitted by a later entry that contains it.
mkdir -p "${WORK_DIR}/area/backups"
cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/area/backups</allowed_path>
        <allowed_path>${WORK_DIR}/area</allowed_path>
    </backups>
</clickhouse>
EOF
rm -rf "${WORK_DIR}/data"
echo "overlapping"
${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/config.xml" --path "${WORK_DIR}/data" -m -q "
    CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t SELECT number FROM numbers(10);
    BACKUP TABLE t TO File('b3') FORMAT Null;
"
ls "${WORK_DIR}/area/backups"
${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/config.xml" --path "${WORK_DIR}/data" \
    -q "BACKUP TABLE t TO File('../../escape') FORMAT Null" 2>&1 | grep -o -m1 BAD_ARGUMENTS

cd "${CUR_DIR}" && rm -rf "${WORK_DIR}"
