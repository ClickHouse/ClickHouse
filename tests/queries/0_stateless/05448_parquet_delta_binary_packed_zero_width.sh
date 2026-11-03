#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
EXPECTED_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"
trap 'rm -f "$DATA_FILE" "$EXPECTED_FILE"' EXIT

# Cover arithmetic progressions, signed wraparound, transitions to nonzero residuals and
# the stateful length/prefix callbacks used by the delta string encodings.
python3 - "$DATA_FILE" "$EXPECTED_FILE" <<'PY'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

def signed(value, bits):
    return (value + (1 << (bits - 1))) % (1 << bits) - (1 << (bits - 1))

rows = [
    (i, -17, 1000000000000 + 17 * i, 12345 - 11 * i,
     signed((1 << 31) - 20 + 13 * i, 32),
     signed((1 << 63) - 40 + 29 * i, 64),
     i * 3 + (i % 7 if (i // 128) % 2 else 0),
     None if i % 11 == 0 else -1000000000000 + i * 19,
     'a' * (1 + i % 129), 'b' * (1 + i % 129) + 'c')
    for i in range(4099)
]
schema = pa.schema([
    pa.field('id', pa.int32(), nullable=False),
    pa.field('constant', pa.int32(), nullable=False),
    pa.field('rising', pa.int64(), nullable=False),
    pa.field('falling', pa.int32(), nullable=False),
    pa.field('wrap32', pa.int32(), nullable=False),
    pa.field('wrap64', pa.int64(), nullable=False),
    pa.field('mixed', pa.int32(), nullable=False),
    pa.field('nullable', pa.int64()),
    pa.field('lengths', pa.string(), nullable=False),
    pa.field('prefixes', pa.string(), nullable=False),
])
table = pa.Table.from_arrays([pa.array(values, type=f.type) for f, values in zip(schema, zip(*rows))], schema=schema)
encodings = {name: 'DELTA_BINARY_PACKED' for name in schema.names}
encodings.update(id='PLAIN', lengths='DELTA_LENGTH_BYTE_ARRAY', prefixes='DELTA_BYTE_ARRAY')
pq.write_table(table, sys.argv[1], use_dictionary=False, compression='NONE', row_group_size=2049,
               column_encoding=encodings)
with open(sys.argv[2], 'w') as out:
    for row in rows:
        out.write('\t'.join('\\N' if value is None else str(value) for value in row) + '\n')
PY

SCHEMA='id Int32, constant Int32, rising Int64, falling Int32, wrap32 Int32, wrap64 Int64, mixed Int32, nullable Nullable(Int64), lengths String, prefixes String'
for BLOCK_SIZE in 1 33 129 513; do
    echo "block size ${BLOCK_SIZE}"
    for PREDICATE in '1' 'id % 257 IN (0, 31, 32, 127, 128, 256)'; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count(), countIf(NOT isNotDistinctFrom(
                tuple(a.constant, a.rising, a.falling, a.wrap32, a.wrap64, a.mixed, a.nullable, a.lengths, a.prefixes),
                tuple(e.constant, e.rising, e.falling, e.wrap32, e.wrap64, e.mixed, e.nullable, e.lengths, e.prefixes)))
            FROM (SELECT * FROM file('$DATA_FILE', Parquet) PREWHERE $PREDICATE) AS a
            INNER JOIN file('$EXPECTED_FILE', TSV, '$SCHEMA') AS e ON a.id = e.id
            SETTINGS input_format_parquet_max_block_size = $BLOCK_SIZE, max_threads = 1"
    done
done
