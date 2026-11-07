#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without the codec parameter, `estimateCompressionRatio` uses the codec the server gives a large merged part: the
# `default_compression_codec` MergeTree setting when it is set, as `MergeTreeData::getCompressionCodecForPart` does.
# CI sets this setting at random on the server, so the codec is passed explicitly to `clickhouse-local` here.

query="SELECT estimateCompressionRatio(x) = estimateCompressionRatio('ZSTD(1)')(x), estimateCompressionRatio(x) != estimateCompressionRatio('LZ4')(x) FROM (SELECT number * 7919 AS x FROM numbers(100000))"

$CLICKHOUSE_LOCAL --query "$query" -- --merge_tree.default_compression_codec='ZSTD(1)'

query="SELECT estimateCompressionRatio(x) = estimateCompressionRatio('LZ4')(x), estimateCompressionRatio(x) != estimateCompressionRatio('ZSTD(1)')(x) FROM (SELECT number * 7919 AS x FROM numbers(100000))"

$CLICKHOUSE_LOCAL --query "$query" -- --merge_tree.default_compression_codec='LZ4'
