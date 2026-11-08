#!/usr/bin/env bash

# Materialization of a projection text index calculates it in several batches when the part is large.
# The calculated blocks used to be squashed together, which concatenated in-memory posting lists of the
# same term incorrectly, and mixed the empty placeholder of a tokenless batch with the posting lists of
# the other batches, causing a segmentation fault while writing the projection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mutations take these settings from the global context, so they are passed on the command line.
$CLICKHOUSE_LOCAL --min_insert_block_size_rows=1000 --min_insert_block_size_bytes=0 --max_block_size=1000 --query "
SET allow_experimental_projection_text_index = 1, enable_full_text_index = 1, mutations_sync = 2;

CREATE TABLE t (id UInt64, s String, s2 String MATERIALIZED s) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

-- The first batches have no tokens, then the term 'rare' occurs once per batch.
INSERT INTO t SELECT number, multiIf(number < 5000, '', number % 1000 = 0, 'rare common word', 'common word') FROM numbers(30000)
    SETTINGS max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

ALTER TABLE t ADD PROJECTION p INDEX s TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE t ADD PROJECTION p2 INDEX s2 TYPE text(tokenizer = splitByNonAlpha, enable_phrase_query_support = 1);
ALTER TABLE t MATERIALIZE PROJECTION p;
ALTER TABLE t MATERIALIZE PROJECTION p2;

SELECT count() FROM t WHERE hasToken(s, 'rare');
SELECT count() FROM t WHERE hasToken(s, 'common');
SELECT count() FROM t WHERE hasPhrase(s2, 'rare common');
SELECT count() FROM t WHERE hasPhrase(s2, 'common word');
SELECT count() FROM t WHERE hasToken(s, 'rare') SETTINGS use_skip_indexes = 0;

OPTIMIZE TABLE t FINAL;

SELECT count() FROM t WHERE hasToken(s, 'rare');
SELECT count() FROM t WHERE hasPhrase(s2, 'rare common');
"
