// Test module for the input splitting of the `ColumnBinary` WASM ABI.
//
// Exported functions:
//   batch_row_count_col(...) -> UInt64 — the number of rows the host put in this call,
//     repeated for every row, so a query can observe the batch boundaries the splitter
//     chose. The input payload is never read, so the export accepts any argument list;
//     tests vary the column count to exercise the frame's per-column metadata.
//
// Kept in its own module rather than added to `columnar_abi.c` so that the artifacts the
// other `ColumnBinary` tests run against stay byte-for-byte unchanged.

#include <stdint.h>
#include <stddef.h>

static void * wmemcpy(void * dst, const void * src, uint32_t n) {
    uint8_t * d = dst; const uint8_t * s = src;
    for (uint32_t i = 0; i < n; ++i) d[i] = s[i];
    return dst;
}
static void wmemset(void * dst, uint8_t val, uint32_t n) {
    uint8_t * d = dst;
    for (uint32_t i = 0; i < n; ++i) d[i] = val;
}
#define memcpy wmemcpy
#define memset wmemset

// ── Minimal allocator ─────────────────────────────────────────────────────────

#define HEAP_SIZE (1 << 16) // 64 KiB, well under the declared initial linear memory
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

typedef struct {
    uint8_t * data;
    uint32_t  size;
    uint32_t  capacity;
} Buffer;

#define MAX_BUFS 64
static Buffer bufs[MAX_BUFS];
static uint32_t buf_pos = 0;
static uint32_t live_buffers = 0;

__attribute__((export_name("clickhouse_create_buffer")))
Buffer * clickhouse_create_buffer(uint32_t size) {
    uint32_t aligned = (size + 15u) & ~15u;
    if (buf_pos >= MAX_BUFS) return NULL;
    if (aligned < size || aligned > HEAP_SIZE - heap_pos) return NULL;
    Buffer * b = &bufs[buf_pos++];
    b->data     = heap + heap_pos;
    b->size     = size;
    b->capacity = aligned;
    heap_pos += aligned;
    live_buffers++;
    return b;
}

__attribute__((export_name("clickhouse_destroy_buffer")))
void clickhouse_destroy_buffer(Buffer * ptr) {
    (void)ptr;
    /* Bump allocator: reclaim everything once the host has released both the input and
       the result buffer of a call, so consecutive batches on the same compartment do not
       exhaust the heap. */
    if (live_buffers > 0 && --live_buffers == 0) {
        heap_pos = 0;
        buf_pos = 0;
    }
}

// ── ColumnBinary wire constants ────────────────────────────────────────────────
// Must match src/Formats/ColumnBinaryWire.h exactly.

#define COL_FIXED64    4u

#define HEADER_BYTES   16u
#define FRAME_MAGIC    0x4E494243u  /* 'C','B','I','N' */
#define FRAME_VERSION  1u
#define DESC_BYTES    40u  // 5 × uint64_t fields

typedef struct {
    uint64_t type;
    uint64_t null_offset;
    uint64_t offsets_offset;
    uint64_t data_offset;
    uint64_t data_size;
} ColDesc;

// Frame header: [4 B magic | 2 B version | 2 B reserved | 4 B num_rows | 4 B num_cols].
static void write_header(uint8_t * p, uint32_t num_rows, uint32_t num_cols) {
    uint32_t magic = FRAME_MAGIC;
    uint16_t version = (uint16_t)FRAME_VERSION;
    uint16_t reserved = 0;
    memcpy(p,      &magic,    4);
    memcpy(p + 4,  &version,  2);
    memcpy(p + 6,  &reserved, 2);
    memcpy(p + 8,  &num_rows, 4);
    memcpy(p + 12, &num_cols, 4);
}

__attribute__((export_name("batch_row_count_col")))
Buffer * batch_row_count_col(Buffer * ptr, uint32_t num_rows) {
    (void)ptr;
    uint32_t data_size = num_rows * 8u;
    uint32_t total = HEADER_BYTES + DESC_BYTES + data_size;

    Buffer * out = clickhouse_create_buffer(total);
    if (!out) return 0;
    memset(out->data, 0, total);
    out->size = total;

    write_header(out->data, num_rows, 1u);

    ColDesc d = {0};
    d.type        = COL_FIXED64;
    d.data_offset = HEADER_BYTES + DESC_BYTES;
    d.data_size   = data_size;
    memcpy(out->data + HEADER_BYTES, &d, DESC_BYTES);

    uint64_t * res = (uint64_t *)(out->data + HEADER_BYTES + DESC_BYTES);
    uint64_t count = num_rows;
    for (uint32_t i = 0; i < num_rows; ++i)
        res[i] = count;

    return out;
}
