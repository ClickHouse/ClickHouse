#!/bin/sh

./clickhouse-compressor < compressor > compressor.compressed
./clickhouse-compressor -d < compressor.compressed > compressor2
cmp compressor compressor2 && echo "Ok." || echo "Fail."
