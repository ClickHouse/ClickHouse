// Test module for COLUMNAR_V1 WASM ABI.
//
// Exported functions:
//   str_byte_sum_col(s String) -> UInt64   — sum of all byte values in each string
//   bytes_equal_col(a String, b String) -> UInt8 — 1 if byte-equal, 0 otherwise
//   add_offset_col(s String, n UInt64) -> UInt64 — sum bytes + n (tests const column)
//   bytes_reverse_col(s String) -> Nullable(String) — reverses bytes of each string

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
static int wmemcmp(const void * a, const void * b, uint32_t n) {
    const uint8_t * p = a; const uint8_t * q = b;
    for (uint32_t i = 0; i < n; ++i) {
        if (p[i] != q[i]) return (int)p[i] - (int)q[i];
    }
    return 0;
}
#define memcpy wmemcpy
#define memset wmemset
#define memcmp wmemcmp

// ── Minimal allocator ─────────────────────────────────────────────────────────

#define HEAP_SIZE (1 << 22) // 4 MB
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

typedef struct {
    uint8_t * data;
    uint32_t  size;
    uint32_t  capacity;
} Buffer;

#define MAX_BUFS 128
static Buffer bufs[MAX_BUFS];
static uint32_t buf_pos = 0;

__attribute__((export_name("clickhouse_create_buffer")))
Buffer * clickhouse_create_buffer(uint32_t size) {
    if (buf_pos >= MAX_BUFS) return NULL;
    uint32_t aligned = (size + 15u) & ~15u;
    if (heap_pos + aligned > HEAP_SIZE) return NULL;
    Buffer * b = &bufs[buf_pos++];
    b->data     = heap + heap_pos;
    b->size     = size;
    b->capacity = aligned;
    heap_pos += aligned;
    return b;
}

__attribute__((export_name("clickhouse_destroy_buffer")))
void clickhouse_destroy_buffer(uint8_t * ptr) { (void)ptr; }

// Grow buffer by appending bytes (realloc-free: uses capacity slack).
static int buf_append(Buffer * b, const uint8_t * src, uint32_t n) {
    if (b->size + n > b->capacity) return 0;
    memcpy(b->data + b->size, src, n);
    b->size += n;
    return 1;
}
static int buf_push(Buffer * b, uint8_t byte) {
    return buf_append(b, &byte, 1);
}

// ── COLUMNAR_V1 wire constants ────────────────────────────────────────────────

#define COL_BYTES      0u
#define COL_NULL_BYTES 1u
#define COL_FIXED8     2u
#define COL_FIXED64    6u
#define COL_IS_CONST   0x80u

#define HEADER_BYTES   8u
#define DESC_BYTES    20u

typedef struct {
    uint32_t type;
    uint32_t null_offset;
    uint32_t offsets_offset;
    uint32_t data_offset;
    uint32_t data_size;
} ColDesc;

// ── Input accessors ───────────────────────────────────────────────────────────

typedef struct {
    uint32_t        num_rows;
    uint32_t        num_cols;
    const uint8_t * base;
} ColBuf;

static ColBuf parse_input(const Buffer * b) {
    ColBuf cb;
    memcpy(&cb.num_rows, b->data,     4);
    memcpy(&cb.num_cols, b->data + 4, 4);
    cb.base = b->data;
    return cb;
}

static ColDesc get_desc(const ColBuf * cb, uint32_t col) {
    ColDesc d;
    memcpy(&d, cb->base + HEADER_BYTES + col * DESC_BYTES, DESC_BYTES);
    return d;
}

// Return pointer to string bytes and their length for string column `col`, row `row`.
// Wire format: null terminator present, len = offsets[idx+1] - offsets[idx] - 1.
static const uint8_t * str_bytes(const ColBuf * cb, uint32_t col,
                                  uint32_t row, uint32_t * out_len) {
    ColDesc d = get_desc(cb, col);
    uint32_t idx = (d.type & COL_IS_CONST) ? 0u : row;
    const uint32_t * offs =
        (const uint32_t *)(cb->base + d.offsets_offset);
    uint32_t start = offs[idx];
    uint32_t end   = offs[idx + 1];
    uint32_t len   = (end > start + 1u) ? end - start - 1u : 0u;
    *out_len = len;
    return cb->base + d.data_offset + start;
}

static uint64_t get_u64(const ColBuf * cb, uint32_t col, uint32_t row) {
    ColDesc d = get_desc(cb, col);
    uint32_t idx = (d.type & COL_IS_CONST) ? 0u : row;
    uint64_t v;
    memcpy(&v, cb->base + d.data_offset + idx * 8u, 8u);
    return v;
}

// ── Output builders ───────────────────────────────────────────────────────────

// Allocate output buffer for a single fixed-width column.
static Buffer * alloc_fixed_out(uint32_t num_rows, uint32_t col_type,
                                 uint32_t elem_size) {
    uint32_t data_size = num_rows * elem_size;
    uint32_t total     = HEADER_BYTES + DESC_BYTES + data_size;
    Buffer * out = clickhouse_create_buffer(total);
    if (!out) return NULL;

    uint8_t * p = out->data;
    memset(p, 0, total);

    memcpy(p,     &num_rows, 4);
    uint32_t one = 1;
    memcpy(p + 4, &one, 4);

    ColDesc d = {0};
    d.type        = col_type;
    d.data_offset = HEADER_BYTES + DESC_BYTES;
    d.data_size   = data_size;
    memcpy(p + HEADER_BYTES, &d, DESC_BYTES);

    out->size = total;
    return out;
}

// ── Exported columnar functions ───────────────────────────────────────────────

// str_byte_sum(s String) -> UInt64
// Computes the sum of all byte values in each string row.
__attribute__((export_name("str_byte_sum_col")))
Buffer * str_byte_sum_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf  cb  = parse_input(ptr);
    Buffer * out = alloc_fixed_out(num_rows, COL_FIXED64, 8u);
    if (!out) return NULL;

    uint64_t * res = (uint64_t *)(out->data + HEADER_BYTES + DESC_BYTES);
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t len;
        const uint8_t * data = str_bytes(&cb, 0, i, &len);
        uint64_t sum = 0;
        for (uint32_t j = 0; j < len; ++j)
            sum += data[j];
        res[i] = sum;
    }
    return out;
}

// bytes_equal(a String, b String) -> UInt8
// Returns 1 if both strings are byte-for-byte equal.
__attribute__((export_name("bytes_equal_col")))
Buffer * bytes_equal_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf  cb  = parse_input(ptr);
    Buffer * out = alloc_fixed_out(num_rows, COL_FIXED8, 1u);
    if (!out) return NULL;

    uint8_t * res = out->data + HEADER_BYTES + DESC_BYTES;
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t la, lb;
        const uint8_t * a = str_bytes(&cb, 0, i, &la);
        const uint8_t * b = str_bytes(&cb, 1, i, &lb);
        res[i] = (la == lb && (la == 0 || memcmp(a, b, la) == 0)) ? 1u : 0u;
    }
    return out;
}

// add_offset(s String, n UInt64) -> UInt64
// Returns byte-sum(s) + n. Used to test COL_IS_CONST handling when n is constant.
__attribute__((export_name("add_offset_col")))
Buffer * add_offset_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf  cb  = parse_input(ptr);
    Buffer * out = alloc_fixed_out(num_rows, COL_FIXED64, 8u);
    if (!out) return NULL;

    uint64_t * res = (uint64_t *)(out->data + HEADER_BYTES + DESC_BYTES);
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t len;
        const uint8_t * data = str_bytes(&cb, 0, i, &len);
        uint64_t sum = 0;
        for (uint32_t j = 0; j < len; ++j)
            sum += data[j];
        res[i] = sum + get_u64(&cb, 1, i);
    }
    return out;
}

// bytes_reverse(s String) -> Nullable(String)
// Returns the bytes of each string reversed. Empty string maps to NULL.
__attribute__((export_name("bytes_reverse_col")))
Buffer * bytes_reverse_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf cb = parse_input(ptr);

    // Layout: [BufHeader][ColDesc][null_map:u8[N]][offsets:u32[N+1]][data...]
    // Pre-allocate generously; actual sizes filled in as we append.
    uint32_t null_base  = HEADER_BYTES + DESC_BYTES;
    uint32_t offs_base  = null_base + num_rows;
    offs_base = (offs_base + 3u) & ~3u; // align to 4
    uint32_t data_base  = offs_base + (num_rows + 1u) * 4u;

    // Upper bound on data: same size as input.
    ColDesc in_desc = get_desc(&cb, 0);
    uint32_t max_data = in_desc.data_size + num_rows; // +num_rows for extra '\0's
    uint32_t total_cap = data_base + max_data;

    Buffer * out = clickhouse_create_buffer(total_cap);
    if (!out) return NULL;
    memset(out->data, 0, total_cap);
    out->size = data_base; // grow as we append

    // Write header
    uint8_t * p = out->data;
    memcpy(p, &num_rows, 4);
    uint32_t one = 1;
    memcpy(p + 4, &one, 4);

    // Wire the descriptor
    ColDesc od = {0};
    od.type           = COL_NULL_BYTES;
    od.null_offset    = null_base;
    od.offsets_offset = offs_base;
    od.data_offset    = data_base;
    od.data_size      = 0; // filled below
    memcpy(p + HEADER_BYTES, &od, DESC_BYTES);

    // offsets[0] = 0 (already zero from memset)
    uint32_t wire_pos = 0;
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t len;
        const uint8_t * src = str_bytes(&cb, 0, i, &len);
        uint8_t * null_map = p + null_base;

        if (len == 0) {
            // NULL output
            null_map[i] = 1;
            // push one '\0' for the empty slot
            p[data_base + wire_pos] = 0;
            wire_pos++;
        } else {
            null_map[i] = 0;
            // reverse copy + null terminator
            for (uint32_t j = len; j > 0; --j)
                p[data_base + wire_pos++] = src[j - 1];
            p[data_base + wire_pos++] = 0; // null terminator
        }
        // store cumulative offset
        memcpy(p + offs_base + (i + 1u) * 4u, &wire_pos, 4u);
    }

    // Patch data_size in descriptor
    od.data_size = wire_pos;
    memcpy(p + HEADER_BYTES, &od, DESC_BYTES);
    out->size = data_base + wire_pos;

    return out;
}
