#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A top-level UNION DISTINCT keeps its output options (INTO OUTFILE, FORMAT, SETTINGS) through the
# normalization that runs before CREATE / ALTER HANDLER stores the query.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
F="additional_table_filters = {'${DB}.t': 'a = 1'}"
HANDLERS=(hd hn he hm hc ha ho ht)

cleanup() {
    local drops=""
    for h in "${HANDLERS[@]}"; do
        drops+="DROP HANDLER IF EXISTS \`${h}_${DB}\`; "
    done
    drops+="DROP TABLE IF EXISTS ${DB}.t"
    $CLICKHOUSE_CLIENT -q "$drops"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "
CREATE TABLE ${DB}.t (a UInt8) ENGINE = Memory;
INSERT INTO ${DB}.t VALUES (1), (2), (3);
CREATE HANDLER \`hd_${DB}\` URL '${P}/distinct' AS SELECT a FROM ${DB}.t UNION DISTINCT SELECT a FROM ${DB}.t FORMAT TSV SETTINGS $F;
CREATE HANDLER \`hn_${DB}\` URL '${P}/noformat' AS (SELECT a FROM ${DB}.t) UNION DISTINCT (SELECT a FROM ${DB}.t) SETTINGS $F;
CREATE HANDLER \`he_${DB}\` URL '${P}/except' AS SELECT a FROM ${DB}.t EXCEPT DISTINCT SELECT 9 UNION DISTINCT SELECT a FROM ${DB}.t FORMAT TSV SETTINGS $F;
CREATE HANDLER \`hc_${DB}\` URL '${P}/csv' AS SELECT 1, 2 UNION DISTINCT SELECT 3, 4 FORMAT CSV SETTINGS format_csv_delimiter = ';';
CREATE HANDLER \`ha_${DB}\` URL '${P}/alter' AS SELECT 1;
ALTER HANDLER \`ha_${DB}\` AS SELECT a FROM ${DB}.t UNION DISTINCT SELECT a FROM ${DB}.t FORMAT TSV SETTINGS $F;
CREATE HANDLER \`ho_${DB}\` URL '${P}/outfile' AS SELECT 1 UNION DISTINCT SELECT 2 INTO OUTFILE 'out.tsv' APPEND AND STDOUT COMPRESSION 'gzip' LEVEL 3 FORMAT TSV SETTINGS max_threads = 1;
CREATE HANDLER \`ht_${DB}\` URL '${P}/truncate' AS SELECT 1 UNION DISTINCT SELECT 2 INTO OUTFILE 'out.tsv' TRUNCATE FORMAT TSV"

$CLICKHOUSE_CLIENT --union_default_mode=DISTINCT -q "CREATE HANDLER \`hm_${DB}\` URL '${P}/default_mode' AS SELECT a FROM ${DB}.t UNION SELECT a FROM ${DB}.t FORMAT TSV SETTINGS $F"

for path in distinct noformat except default_mode csv alter; do
    echo "--- $path"
    ${CLICKHOUSE_CURL} -sS "${BASE}${P}/${path}" | sort
done

echo "--- stored"
$CLICKHOUSE_CLIENT -q "SELECT query FROM system.handlers WHERE name IN ('ho_${DB}', 'ht_${DB}') ORDER BY name FORMAT TSVRaw"

echo "--- http"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 1 UNION DISTINCT SELECT 2 INTO OUTFILE '${DB}.tsv'" | grep -o INTO_OUTFILE_NOT_ALLOWED | sort -u
