#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INSERT ... RETURNING ... FROM INFILE` in `clickhouse-local` reads the data from the file, not from stdin.

prefix="${CLICKHOUSE_TMP}/05442_${CLICKHOUSE_DATABASE}"
echo -e '10\n20' > "${prefix}_a.tsv"
echo -e '30\n40' | gzip > "${prefix}_b.tsv.gz"

echo "Empty stdin"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64, y UInt64 DEFAULT x * 2) ENGINE = Memory;
    INSERT INTO t (x) RETURNING (SELECT count(), sum(x), sum(y)) FROM INFILE '${prefix}_a.tsv' FORMAT TSV;
    SELECT count(), sum(x), sum(y) FROM t;
" < /dev/null

echo "Non-empty stdin"
echo 99 | ${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t RETURNING (SELECT count(), sum(x)) FROM INFILE '${prefix}_a.tsv' FORMAT TSV;
    SELECT count(), sum(x) FROM t;
"

echo "Compression, glob and the format from the file name"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t RETURNING (SELECT x ORDER BY x) FROM INFILE '${prefix}_b.tsv.gz';
    INSERT INTO t RETURNING (SELECT count(), sum(x)) FROM INFILE '${prefix}_*' COMPRESSION 'auto' FORMAT TSV;
    SELECT count(), sum(x) FROM t;
" < /dev/null

rm -f "${prefix}_a.tsv" "${prefix}_b.tsv.gz"
