#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"

function run()
{
    local err
    if err=$(${CLICKHOUSE_LOCAL} --config-file "$config" --query "$1" 2>&1 >/dev/null); then
        echo "ok: $1"
    else
        echo "$(echo "$err" | grep -o -E '\([A-Z_]+\)' | head -n1): $1"
    fi
}

cat > "$config" <<CONFIG
<clickhouse>
    <factory_restrictions>
        <formats><deny><csv/></deny></formats>
        <table_engines><allow><Memory/><MergeTree/><Dictionary/></allow></table_engines>
        <table_functions><deny><numbers/></deny></table_functions>
        <functions><deny><lower/></deny></functions>
        <aggregate_functions><deny><sum/></deny></aggregate_functions>
        <aggregate_function_combinators><deny><If/></deny></aggregate_function_combinators>
        <data_types><deny><Float32/></deny></data_types>
        <compression_codecs><deny><Delta/></deny></compression_codecs>
        <dictionary_layouts><deny><flat/></deny></dictionary_layouts>
        <skip_indexes><deny><bloom_filter/></deny></skip_indexes>
        <statistics><deny><tdigest/></deny></statistics>
    </factory_restrictions>
</clickhouse>
CONFIG

run "SELECT 1 FORMAT CSV"
run "SELECT 1 FORMAT TSV"
run "SELECT * FROM format(CSV, '1')"
run "CREATE TABLE t (x UInt8) ENGINE = Memory"
run "CREATE TABLE t (x UInt8) ENGINE = Log"
run "SELECT * FROM numbers(1)"
run "SELECT * FROM system.one"
run "SELECT lcase('A')"
run "SELECT LOWER('A')"
run "SELECT upper('a')"
run "SELECT sum(1)"
run "SELECT sumArray([1])"
run "SELECT countIf(1)"
run "SELECT count()"
run "SELECT CAST(1 AS Float32)"
run "SELECT CAST(1 AS FLOAT)"
run "SELECT CAST(1 AS Float64)"
run "CREATE TABLE t (x UInt8 CODEC(Delta)) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE t (x UInt8 CODEC(LZ4)) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE t (x UInt8, INDEX i x TYPE bloom_filter) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE t (x UInt8, INDEX i x TYPE minmax) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE t (x UInt8 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE t (x UInt8 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x"
run "CREATE DICTIONARY d (x UInt64, y UInt64) PRIMARY KEY x SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0); SELECT dictGet('d', 'y', 1)"
run "CREATE DICTIONARY d (x UInt64, y UInt64) PRIMARY KEY x SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0); SELECT dictGet('d', 'y', 1)"

${CLICKHOUSE_LOCAL} --config-file "$config" --query "SELECT 1 FORMAT CSV" 2>&1 | grep -o 'Format `CSV` is disabled by the server configuration (`factory_restrictions.formats`)'

echo "-- invalid configs"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><formats><allow><CSV/></allow><deny><TSV/></deny></formats></factory_restrictions></clickhouse>
CONFIG
run "SELECT 1"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><formats/></factory_restrictions></clickhouse>
CONFIG
run "SELECT 1"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><format><deny><CSV/></deny></format></factory_restrictions></clickhouse>
CONFIG
run "SELECT 1"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><formats><deny><CSV>1</CSV></deny></formats></factory_restrictions></clickhouse>
CONFIG
run "SELECT 1"

echo "-- empty allow list"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><table_functions><allow/></table_functions></factory_restrictions></clickhouse>
CONFIG
run "SELECT * FROM numbers(1)"
run "SELECT * FROM system.one"

rm -f "$config"
