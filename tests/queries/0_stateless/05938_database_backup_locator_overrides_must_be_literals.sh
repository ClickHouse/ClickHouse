#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires S3.
# Tag no-replicated-database: the CREATE DATABASE runs with no user there, so its check is a no-op.

# A `Backup` database evaluates the named collection overrides of its backup destination again when it opens the
# backup, with no user. An override that depends on who evaluates it would make the database read another backup
# than the one authorized at creation, so only literal overrides are accepted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
nc="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
s3="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
base="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/base')"

# `incremental` stores its only row in `base`, `full` stores none.
${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
DROP NAMED COLLECTION IF EXISTS ${nc};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT CREATE DATABASE ON *.* TO ${user};
GRANT TABLE ENGINE ON *.* TO ${user};
GRANT READ ON S3 TO ${user};
GRANT NAMED COLLECTION ON ${nc} TO ${user};
GRANT SELECT ON ${db}_literal.* TO ${user};
CREATE NAMED COLLECTION ${nc} AS access_key_id = 'test', secret_access_key = 'testtest';
CREATE TABLE ${db}.t (x UInt64) ENGINE = MergeTree ORDER BY x COMMENT 'base';
INSERT INTO ${db}.t VALUES (7);
BACKUP TABLE ${db}.t TO ${base} FORMAT Null;
ALTER TABLE ${db}.t MODIFY COMMENT '';
BACKUP TABLE ${db}.t TO S3(${nc}, url = '${s3}/incremental') SETTINGS base_backup = ${base} FORMAT Null;
TRUNCATE TABLE ${db}.t;
BACKUP TABLE ${db}.t TO S3(${nc}, url = '${s3}/full') FORMAT Null;
"

# Prints the error code and the grant that the query of the user lacks, or the word given.
function run()
{
    local output grant
    output=$(${CLICKHOUSE_CLIENT} --user "${user}" -q "$2" 2>&1)
    if echo "${output}" | grep -q -E 'Code: [0-9]+'; then
        grant=$(echo "${output}" | grep -o -m1 -E 'grant [A-Z ]+ ON [A-Za-z]+')
        echo "$(echo "${output}" | grep -o -m1 -E 'Code: [0-9]+')${grant:+ ${grant}}"
    else
        echo "$1"
    fi
}

echo '--- without READ ON FILE, the incremental backup is refused'
run created "CREATE DATABASE ${db}_incremental ENGINE = Backup('${db}', S3(${nc}, url = '${s3}/incremental'))"

echo '--- so is an override that names the full backup only for this user'
run created "CREATE DATABASE ${db}_expression ENGINE = Backup('${db}', S3(${nc},
    url = if(currentUser() = '${user}', '${s3}/full', '${s3}/incremental')))"

echo '--- a literal override is accepted'
run created "CREATE DATABASE ${db}_literal ENGINE = Backup('${db}', S3(${nc}, url = '${s3}/full'))"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count() FROM ${db}_literal.t"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${db}_incremental;
DROP DATABASE IF EXISTS ${db}_expression;
DROP DATABASE IF EXISTS ${db}_literal;
DROP TABLE ${db}.t;
DROP NAMED COLLECTION ${nc};
DROP USER ${user};
"
