#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest, the test uses S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./orc_malformed.lib
. "$CUR_DIR"/orc_malformed.lib

DATA="$CUR_DIR/data_orc"

# The number of the repeated objects in the metadata is limited by the numbers of the columns and the stripes.
orc_check footer_statistics "$DATA/malformed_footer_too_many_statistics.orc" 'a UInt64, b String' "SELECT count(), sum(length(b)) FROM SOURCE" \
    'Too many column statistics in ORC footer: 13, the file has 3 columns'
orc_check stripe_statistics "$DATA/malformed_metadata_too_many_stripes.orc" 'a UInt64, b String' "SELECT count(), sum(length(b)) FROM SOURCE" \
    'Too many stripe statistics in ORC metadata, at most 1 expected'
