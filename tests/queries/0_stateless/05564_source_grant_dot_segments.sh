#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires S3 and Azure
# Tag no-replicated-database: without enable_read_write_grants, replicated access storage drops grant filters

# A source grant filter must only authorize the location the storage accesses: a location the
# normalization used for the check would change (dot segments, empty segments, escapes, a fragment)
# or that is expanded outside its path needs the grant on the whole source, and a glob character
# must match the filter both as written and escaped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="u_05564_${CLICKHOUSE_DATABASE}"
ufp=${USER_FILES_PATH%/}
s3="http://localhost:11111"
container="cont-05564-$(echo "${CLICKHOUSE_DATABASE}" | tr _ -)"
azure_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --multiquery -q "
CREATE TABLE $db.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.t VALUES (1);
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT BACKUP, SELECT ON $db.t TO $user;
GRANT CREATE TABLE, INSERT ON $db.* TO $user;
GRANT READ ON S3('$s3/test/.*') TO $user;
GRANT WRITE ON S3('$s3/test/$db/allowed/.*') TO $user;
GRANT WRITE ON AZURE('$db/backups/.*') TO $user;
GRANT WRITE ON AZURE('$db:.*') TO $user;
GRANT READ ON FILE('$ufp/$db/allowed/.*') TO $user;
GRANT READ ON URL('$s3/test/.*') TO $user;
GRANT READ ON S3('$s3/esc/a%2A.tsv') TO $user;
GRANT READ ON URL('$s3/esc/%7Ba%2Cb%7D.tsv') TO $user;
GRANT READ ON URL('$s3/esc/a%21b.tsv') TO $user;
GRANT READ ON URL('$s3/esc/a%2Cb.tsv') TO $user;
GRANT WRITE ON AZURE('$db/arc/ok.tar') TO $user;
GRANT READ ON S3('$s3/exact/report.csv') TO $user;
GRANT READ ON URL('$s3/exact/report.csv') TO $user;
GRANT READ ON AZURE('$db/exact/report.csv') TO $user;
"

# Prints whether the query was refused for the named grant; any other outcome is "not denied".
function check()
{
    local grant=$1 query=$2
    if ${CLICKHOUSE_CLIENT} --user "$user" -q "$query" 2>&1 | grep -q -F "necessary to have the grant $grant"; then
        echo "denied $grant"
    else
        echo "not denied"
    fi
}

function azure()
{
    echo "AzureBlobStorage('http://localhost:10000/devstoreaccount1', '$container', '$1', 'devstoreaccount1', '$azure_key')"
}

function azure_table()
{
    echo "azureBlobStorage('http://localhost:10000/devstoreaccount1', '$container', '$1', 'devstoreaccount1', '$azure_key', 'CSV')"
}

s3_backup_settings="SETTINGS backup_restore_s3_retry_attempts = 0"

echo "s3 dot segment: $(check 'READ ON S3' "SELECT * FROM s3('$s3/other/../test/a.tsv', 'TSV')")"
echo "s3 escaped dot segment: $(check 'READ ON S3' "SELECT * FROM s3('$s3/other/%2e%2e/test/a.tsv', 'TSV')")"
echo "s3 backup dot segment: $(check 'WRITE ON S3' "BACKUP TABLE $db.t TO S3('$s3/test/$db/denied/../allowed/dotdot.tar', 'clickhouse', 'clickhouse') $s3_backup_settings")"
echo "s3 restore dot segment: $(check 'READ ON S3' "RESTORE TABLE $db.t AS $db.r FROM S3('$s3/other/../test/$db/nothing', 'clickhouse', 'clickhouse') $s3_backup_settings")"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'r'"
echo "s3 dot segment inside the filter: $(check 'READ ON S3' "SELECT * FROM s3('$s3/test/sub/../a.tsv', 'TSV')")"
echo "s3 empty segment: $(check 'READ ON S3' "SELECT * FROM s3('$s3/test//a.tsv', 'TSV')")"
echo "s3 query wildcard: $(check 'READ ON S3' "SELECT * FROM s3('$s3/test/a.ts?', 'TSV')")"
echo "s3 escape without a query: $(check 'READ ON S3' "SELECT * FROM s3('$s3/%74est/a.tsv', 'TSV')")"
echo "azure dot segment: $(check 'WRITE ON AZURE' "BACKUP TABLE $db.t TO $(azure "$db/other/../backups/x")")"
echo "azure escape: $(check 'WRITE ON AZURE' "BACKUP TABLE $db.t TO $(azure "$db%2Fbackups/y")")"
echo "azure scheme-like name: $(check 'WRITE ON AZURE' "BACKUP TABLE $db.t TO $(azure "${db^^}:x")")"
echo "file escape: $(check 'READ ON FILE' "SELECT * FROM icebergLocal('$ufp/$db%2Fallowed/t')")"
echo "url dot segment: $(check 'READ ON URL' "SELECT * FROM url('$s3/other/../test/a.tsv', 'TSV')")"
echo "url escape: $(check 'READ ON URL' "SELECT * FROM url('$s3/%74est/a.tsv', 'TSV')")"
echo "azure archive escaped dot segments: $(check 'WRITE ON AZURE' "BACKUP TABLE $db.t TO $(azure "$db/backups/a%2F..%2F..%2Fz.tar")")"
echo "url glob alternative: $(check 'READ ON URL' "SELECT * FROM url('$s3/test/{a,..}/a.tsv', 'TSV')")"
echo "s3 escaped wildcard inside the filter: $(check 'READ ON S3' "SELECT * FROM s3('$s3/test/a.ts%3F', 'TSV')")"
echo "s3 glob against an escaped filter: $(check 'READ ON S3' "SELECT * FROM s3('$s3/esc/a*.tsv', 'TSV')")"
echo "url glob against an escaped filter: $(check 'READ ON URL' "SELECT * FROM url('$s3/esc/{a,b}.tsv', 'TSV')")"
echo "s3 fragment: $(check 'READ ON S3' "SELECT * FROM s3('$s3/test/a.tsv#x', 'TSV')")"
echo "url fragment: $(check 'READ ON URL' "SELECT * FROM url('$s3/test/a.tsv#x', 'TSV')")"
echo "s3 glob: $(check 'READ ON S3' "SELECT * FROM s3('$s3/test/$db/*.tsv', 'TSV')")"
echo "url glob alternatives: $(check 'READ ON URL' "SELECT * FROM url('$s3/test/$db/{a,b}.tsv', 'TSV')")"
echo "file: $(check 'READ ON FILE' "SELECT * FROM icebergLocal('$ufp/$db/allowed/t')")"
echo "azure archive: $(check 'WRITE ON AZURE' "BACKUP TABLE $db.t TO $(azure "$db/arc/ok.tar")")"
echo "s3 exact filter: $(check 'READ ON S3' "SELECT * FROM s3('$s3/exact/report.csv', 'CSV')")"
echo "s3 glob against an exact filter: $(check 'READ ON S3' "SELECT * FROM s3('$s3/exact/report*csv', 'CSV')")"
echo "s3 escaped wildcard against an exact filter: $(check 'READ ON S3' "SELECT * FROM s3('$s3/exact/report%3Fcsv', 'CSV')")"
echo "url failover against an exact filter: $(check 'READ ON URL' "SELECT * FROM url('$s3/exact/report|csv', 'CSV')")"
echo "azure glob against an exact filter: $(check 'READ ON AZURE' "SELECT * FROM $(azure_table "$db/exact/report*csv")")"
echo "url failover in the query: $(check 'READ ON URL' "SELECT * FROM url('$s3/test/a.tsv?x|$s3/other/a.tsv', 'TSV')")"
echo "url glob in the query: $(check 'READ ON URL' "SELECT * FROM url('$s3/test/a.tsv?{x,y}', 'TSV')")"
echo "file empty segment: $(check 'READ ON FILE' "SELECT * FROM icebergLocal('$ufp/$db/allowed//t')")"
echo "azure empty segment: $(check 'WRITE ON AZURE' "BACKUP TABLE $db.t TO $(azure "$db/backups//z")")"
echo "url raw character against an escaped filter: $(check 'READ ON URL' "SELECT * FROM url('$s3/esc/a!b.tsv', 'TSV')")"
echo "url raw comma against an escaped filter: $(check 'READ ON URL' "SELECT * FROM url('$s3/esc/a,b.tsv', 'TSV')")"

${CLICKHOUSE_CLIENT} -q "GRANT READ ON S3 TO $user"
echo "s3 dot segment with the whole-source grant: $(check 'READ ON S3' "SELECT * FROM s3('$s3/other/../test/a.tsv', 'TSV')")"
