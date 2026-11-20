#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"

function run()
{
    local err
    if err=$(${CLICKHOUSE_LOCAL} --config-file "$config" --query "$1" 2>&1 >/dev/null </dev/null); then
        echo "ok: $1"
    else
        echo "$(echo "$err" | grep -o -E '\([A-Z_]+\)' | head -n1): $1"
    fi
}

cat > "$config" <<CONFIG
<clickhouse>
    <factory_restrictions>
        <formats><deny><csv/><JSONEachRow/><TabSeparatedWithNames/></deny></formats>
        <table_engines><allow><Memory/><MergeTree/><Dictionary/></allow></table_engines>
        <table_functions><deny><numbers/></deny></table_functions>
        <functions><deny><lower/></deny></functions>
        <aggregate_functions><deny><sum/><quantile/></deny></aggregate_functions>
        <aggregate_function_combinators><deny><If/></deny></aggregate_function_combinators>
        <data_types><deny><Float32/><Enum8/><Tuple/></deny></data_types>
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
run "SELECT 1 AS x FORMAT JSONL"
run "SELECT 1 AS x FORMAT NDJSON"
run "SELECT * FROM format(JSONLines, '{\"x\": 1}')"
run "DESC format(JSONL, '{\"x\": 1}')"
run "SELECT 1 AS x FORMAT TSVWithNames"
run "SELECT 1 AS x FORMAT TSVWithNamesAndTypes"
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
run "SELECT median(1)"
run "SELECT medianIf(1, 1)"
run "SELECT any(1) RESPECT NULLS"
run "SELECT count()"
run "SELECT CAST(1 AS Float32)"
run "SELECT CAST(1 AS FLOAT)"
run "SELECT CAST(1 AS Float64)"
run "SELECT CAST('a' AS Enum8('a' = 1))"
run "SELECT CAST('a' AS Enum('a' = 1))"
run "SELECT CAST('a' AS Enum16('a' = 1))"
run "SELECT CAST((1, 2) AS Tuple(a UInt8, b UInt8))"
run "SELECT CAST((1, 2) AS Tuple(UInt8, UInt8))"
run "CREATE TABLE t (x Enum8('a' = 1)) ENGINE = Memory"
run "CREATE TABLE t (x Tuple(UInt8)) ENGINE = Memory"
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

echo "-- default codec"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><compression_codecs><deny><ZSTD/></deny></compression_codecs></factory_restrictions></clickhouse>
CONFIG
run "SELECT 1"
run "CREATE TABLE t (x UInt8) ENGINE = Log; INSERT INTO t SELECT 1"
run "CREATE TABLE t (x UInt8 CODEC(Default)) ENGINE = MergeTree ORDER BY x"

echo "-- statistics of an existing table"
path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_path"
rm -rf "$path"
echo "<clickhouse></clickhouse>" > "$config"
${CLICKHOUSE_LOCAL} --path "$path" --config-file "$config" --query "
    CREATE TABLE t (x UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t SELECT number FROM numbers(100);"
cat > "$config" <<CONFIG
<clickhouse><factory_restrictions><statistics><deny><tdigest/></deny></statistics></factory_restrictions></clickhouse>
CONFIG
function run_with_path()
{
    local err
    if err=$(${CLICKHOUSE_LOCAL} --path "$path" --config-file "$config" --query "$1" 2>&1 >/dev/null </dev/null); then
        echo "ok: $1"
    else
        echo "$(echo "$err" | grep -o -E '\([A-Z_]+\)' | head -n1): $1"
    fi
}
run_with_path "SELECT count() FROM t"
run_with_path "INSERT INTO t SELECT 1"
run_with_path "OPTIMIZE TABLE t FINAL"
rm -rf "$path"

rm -f "$config"
