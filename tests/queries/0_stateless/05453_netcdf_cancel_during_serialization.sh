#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `NetCDF` format buffers the whole result and serializes it at the end of the query, where the
# executor no longer looks at the cancellation of the query. A query that is killed while the file
# is being written must stop, not deliver the rest of the file.

QUERY_ID="${CLICKHOUSE_DATABASE}_netcdf_cancel"
OUT=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_netcdf_cancel.nc
rm -f "$OUT"

# 4194304 values of 8 bytes are 32 MiB of data. The client reads slowly, so the server is still
# writing the file when the query is killed.
${CLICKHOUSE_CURL} -sS --limit-rate 1M "${CLICKHOUSE_URL}&query_id=${QUERY_ID}&http_wait_end_of_query=0" \
    -d "SELECT number AS x FROM numbers(4194304) FORMAT NetCDF" -o "$OUT" 2>/dev/null &
PID=$!

# Wait for the output to start.
while [ ! -s "$OUT" ] && kill -0 "$PID" 2>/dev/null
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null
wait "$PID"

SIZE=$(stat -c %s "$OUT")
if [ "$SIZE" -gt 0 ] && [ "$SIZE" -lt 33554432 ]
then
    echo "OK"
else
    echo "Unexpected size of the output: $SIZE"
fi

rm -f "$OUT"
