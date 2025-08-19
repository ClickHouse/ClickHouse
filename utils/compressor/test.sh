#!/bin/sh

./clickhouse-compressor < clickhouse-compressor > compressed
./clickhouse-compressor -d < compressed > clickhouse-compressor2
cmp clickhouse-compressor clickhouse-compressor2 && echo "Ok." || echo "Fail."
