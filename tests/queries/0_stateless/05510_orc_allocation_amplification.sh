#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest, the test uses S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./orc_malformed.lib
. "$CUR_DIR"/orc_malformed.lib

DATA="$CUR_DIR/data_orc"

# The memory must be allocated by the size of the data, not by the sizes declared in the file.
LIMIT='max_memory_usage = 100000000'

# The declared compression block size is 1 GiB, the chunks are small. The schema inference reads the compressed footer.
orc_check compression_block "$DATA/malformed_compression_block_1g.orc" '' "SELECT count(), sum(length(s)) FROM SOURCE" '' "$LIMIT"

# The declared dictionary size is 2^32 - 1, the stream has a few entries.
orc_check dictionary_size "$DATA/malformed_dictionary_size_max.orc" 's Nullable(String)' "SELECT count(), sum(length(s)) FROM SOURCE" \
    'Unexpected end of ORC stream column 1 kind 2' "$LIMIT"

# The sizes stored in the snappy and zstd chunks are 1 GiB, the chunks have a few bytes.
orc_check snappy_chunk_size "$DATA/malformed_snappy_chunk_size.orc" 'x Int64' "SELECT count() FROM SOURCE" \
    'snappy-compressed chunk of ORC file decompresses to 1073741824 bytes' "$LIMIT"
orc_check zstd_chunk_size "$DATA/malformed_zstd_chunk_size.orc" 'x Int64' "SELECT count() FROM SOURCE" \
    'Malformed zstd-compressed data in ORC file' "$LIMIT"
