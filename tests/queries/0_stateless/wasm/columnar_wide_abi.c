// Test module for the wide-type half of the `COLUMNAR_V1` WASM ABI: the
// COL_FIXEDN (7) and COL_LOWCARD (8) descriptor tags. Every export echoes the
// input column back as a hex String (lowercase, one byte = two chars, "NULL"
// for a null row), so a SQL assertion can pin the exact bytes the host put on
// the wire without the guest having to re-encode the declared type.
//
// Exported functions (all COLUMNAR_V1: Buffer * f(Buffer *in, uint32 num_rows)):
//   hex_fixed_col(v T)      -> String — T any fixed-width column (COL_FIXED8/16/32/64
//                              or COL_FIXEDN; element width recovered as data_size/rows).
//                              Works for FixedString(N), UUID, IPv6, Decimal128/256, Int128.
//   hex_lc_col(s LowCardinality(String)) -> String — decodes the COL_LOWCARD frame
//                              (index array + embedded dictionary descriptor) and hexes
//                              the row's dictionary value.
//   lc_dict_rows_col(s LowCardinality(String)) -> UInt64 — the dictionary row count read
//                              from the LC header. A split call's second batch re-serializes
//                              its own dictionary, so a per-batch value is expected here.
//   hex_variant_col(v Variant(...)) -> String — decodes the COL_VARIANT frame and returns
//                              "<discriminator>:<hex of the row's value>" ("NULL" for a null
//                              row). Returns NULL (a trap on the host) unless the argument
//                              arrives as one COL_VARIANT column.

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

// ── Minimal allocator (same shape as columnar_abi.c) ─────────────────────────

#define HEAP_SIZE (1 << 21) // 2 MB
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

// ── COLUMNAR_V1 wire constants — must match src/Functions/UserDefined/UserDefinedWebAssemblyColumnarWire.h ──

#define COL_BYTES      0u
#define COL_VARIANT    6u
#define COL_LOWCARD    8u
#define COL_TAG_MASK   0x1Fu
#define COL_IS_NULLABLE 0x20u
#define COL_IS_CONST   0x80u

#define HEADER_BYTES   16u
#define FRAME_MAGIC    0x4E494243u
#define FRAME_VERSION  1u
#define DESC_BYTES     40u

typedef struct {
    uint64_t type;
    uint64_t null_offset;
    uint64_t offsets_offset;
    uint64_t data_offset;
    uint64_t data_size;
} ColDesc;

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

typedef struct {
    uint32_t        num_rows;
    uint32_t        num_cols;
    const uint8_t * base;
} ColBuf;

static ColBuf parse_input(const Buffer * b) {
    ColBuf cb;
    uint32_t magic;
    memcpy(&magic, b->data, 4);
    if (magic != FRAME_MAGIC) { cb.num_rows = 0; cb.base = 0; return cb; }
    memcpy(&cb.num_rows, b->data + 8,  4);
    memcpy(&cb.num_cols, b->data + 12, 4);
    cb.base = b->data;
    return cb;
}

static ColDesc get_desc(const ColBuf * cb, uint32_t col) {
    ColDesc d;
    memcpy(&d, cb->base + HEADER_BYTES + col * DESC_BYTES, DESC_BYTES);
    return d;
}

static uint64_t load_index(const uint8_t * p, uint32_t w) {
    uint64_t v = 0;
    memcpy(&v, p, w); // little-endian host == wire
    return v;
}

// ── Output: one COL_BYTES String column of hex ──────────────────────────────

// Capacity: hex doubles every input byte, so callers pass the exact worst-case
// byte total. Layout: [header][desc][offsets u64[n+1]][data...].
static Buffer * begin_hex_out(uint32_t num_rows, uint32_t max_data) {
    uint32_t offs_base = HEADER_BYTES + DESC_BYTES;
    uint32_t total     = offs_base + (num_rows + 1u) * 8u + max_data;
    Buffer * out = clickhouse_create_buffer(total);
    if (!out) return NULL;
    memset(out->data, 0, total);
    write_header(out->data, num_rows, 1u);
    ColDesc od = {0};
    od.type           = COL_BYTES;
    od.offsets_offset = offs_base;
    od.data_offset    = offs_base + (num_rows + 1u) * 8u;
    memcpy(out->data + HEADER_BYTES, &od, DESC_BYTES);
    out->size = od.data_offset;
    return out;
}

static const char hex_digits[16] = "0123456789abcdef";

// One row appended by hex_row with an explicit cursor owned by the caller.
typedef struct { Buffer * out; uint32_t cursor; } HexOut;

static void hex_row(HexOut * o, uint32_t row, const uint8_t * src, uint32_t n, int is_null) {
    uint8_t * p = o->out->data;
    ColDesc od;
    memcpy(&od, p + HEADER_BYTES, DESC_BYTES);
    uint64_t pos = o->cursor;
    if (is_null) {
        p[od.data_offset + pos++] = 'N'; p[od.data_offset + pos++] = 'U';
        p[od.data_offset + pos++] = 'L'; p[od.data_offset + pos++] = 'L';
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            p[od.data_offset + pos++] = hex_digits[src[i] >> 4];
            p[od.data_offset + pos++] = hex_digits[src[i] & 0xFu];
        }
    }
    memcpy(p + od.offsets_offset + (uint64_t)(row + 1u) * 8u, &pos, 8u);
    o->cursor = (uint32_t)pos;
}

static void finish_hex(HexOut * o) {
    uint8_t * p = o->out->data;
    ColDesc od;
    memcpy(&od, p + HEADER_BYTES, DESC_BYTES);
    od.data_size = o->cursor;
    memcpy(p + HEADER_BYTES, &od, DESC_BYTES);
    o->out->size = od.data_offset + o->cursor;
}

// ── Exports ──────────────────────────────────────────────────────────────────

// hex_fixed_col: any fixed-width column (COL_FIXED8/16/32/64/FIXEDN), optional
// null map, optional const. Element width recovered as data_size / stored_rows,
// exactly as the host's read side does it (UserDefinedWebAssemblyColumnarCodec.cpp, COL_FIXEDN branch).
__attribute__((export_name("hex_fixed_col")))
Buffer * hex_fixed_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf cb = parse_input(ptr);
    if (!cb.base) return NULL;
    ColDesc d = get_desc(&cb, 0);

    uint32_t stored_rows = (d.type & COL_IS_CONST) ? 1u : num_rows;
    if (stored_rows == 0u || (d.data_size % stored_rows) != 0u) return NULL;
    uint32_t width = (uint32_t)(d.data_size / stored_rows);

    HexOut ho; ho.out = begin_hex_out(num_rows, num_rows * width * 2u + 16u);
    if (!ho.out) return NULL;
    ho.cursor = 0;

    for (uint32_t i = 0; i < num_rows; ++i) {
        int is_null = 0;
        if (d.type & COL_IS_NULLABLE) {
            uint8_t n = cb.base[d.null_offset + i];
            if (n) is_null = 1;
        }
        uint32_t idx = (d.type & COL_IS_CONST) ? 0u : i;
        hex_row(&ho, i, cb.base + d.data_offset + (uint64_t)idx * width, width, is_null);
    }
    finish_hex(&ho);
    return ho.out;
}

// hex_lc_col: COL_LOWCARD with a COL_BYTES dictionary. Header at data_offset:
//   uint32 dict_row_count | uint8 index_elem_width | uint8[3] pad | ColDesc dict
// Index array (index_elem_width wide per row) at offsets_offset; the dictionary's
// own string offsets/data at positions given by the embedded dict descriptor.
__attribute__((export_name("hex_lc_col")))
Buffer * hex_lc_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf cb = parse_input(ptr);
    if (!cb.base) return NULL;
    ColDesc d = get_desc(&cb, 0);
    if ((d.type & COL_TAG_MASK) != COL_LOWCARD) return NULL;

    uint32_t dict_rows; memcpy(&dict_rows, cb.base + d.data_offset, 4);
    uint8_t  idx_w = cb.base[d.data_offset + 4];
    if (idx_w != 1u && idx_w != 2u && idx_w != 4u && idx_w != 8u) return NULL;

    ColDesc dict; memcpy(&dict, cb.base + d.data_offset + 8, DESC_BYTES);
    const uint64_t * dict_offs = (const uint64_t *)(cb.base + dict.offsets_offset);
    uint64_t dict_data_bytes = dict.data_size;

    // Worst case every row repeats the whole dictionary's bytes as its value.
    HexOut ho; ho.out = begin_hex_out(num_rows, (uint32_t)(dict_data_bytes * num_rows * 2u + 16u));
    if (!ho.out) return NULL;
    ho.cursor = 0;

    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t idx = (d.type & COL_IS_CONST) ? 0u : i;
        uint64_t e = load_index(cb.base + d.offsets_offset + (uint64_t)idx * idx_w, idx_w);
        if (e >= dict_rows) return NULL;
        uint64_t start = dict_offs[e], end = dict_offs[e + 1];
        hex_row(&ho, i, cb.base + dict.data_offset + start, (uint32_t)(end - start), 0);
    }
    finish_hex(&ho);
    return ho.out;
}

// lc_dict_rows_col: the dictionary row count straight from the LC header.
__attribute__((export_name("lc_dict_rows_col")))
Buffer * lc_dict_rows_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf cb = parse_input(ptr);
    if (!cb.base) return NULL;
    ColDesc d = get_desc(&cb, 0);
    if ((d.type & COL_TAG_MASK) != COL_LOWCARD) return NULL;
    uint32_t dict_rows32; memcpy(&dict_rows32, cb.base + d.data_offset, 4);
    uint64_t dict_rows = dict_rows32;

    uint32_t total = HEADER_BYTES + DESC_BYTES + num_rows * 8u;
    Buffer * out = clickhouse_create_buffer(total);
    if (!out) return NULL;
    memset(out->data, 0, total);
    write_header(out->data, num_rows, 1u);
    ColDesc od = {0};
    od.type = 4u; /* COL_FIXED64 */
    od.data_offset = HEADER_BYTES + DESC_BYTES;
    od.data_size = num_rows * 8u;
    memcpy(out->data + HEADER_BYTES, &od, DESC_BYTES);
    for (uint32_t i = 0; i < num_rows; ++i)
        memcpy(out->data + od.data_offset + (uint64_t)i * 8u, &dict_rows, 8u);
    out->size = total;
    return out;
}

// hex_variant_col: COL_VARIANT. Discriminators (u8, 255 = NULL) at null_offset, u32
// row offsets into the sub-column at offsets_offset. At data_offset:
//   uint32 K | K x { uint8 discriminator | uint8[3] pad | ColDesc sub-column }
// where the sub-column descriptor's null_offset holds its row count.
__attribute__((export_name("hex_variant_col")))
Buffer * hex_variant_col(Buffer * ptr, uint32_t num_rows) {
    ColBuf cb = parse_input(ptr);
    if (!cb.base) return NULL;
    ColDesc d = get_desc(&cb, 0);
    if ((d.type & COL_TAG_MASK) != COL_VARIANT) return NULL;

    ColDesc subs[256];
    uint8_t present[256];
    memset(present, 0, sizeof(present));
    uint32_t k; memcpy(&k, cb.base + d.data_offset, 4);
    for (uint32_t r = 0; r < k; ++r) {
        const uint8_t * rec = cb.base + d.data_offset + 4u + r * (4u + DESC_BYTES);
        memcpy(&subs[rec[0]], rec + 4, DESC_BYTES);
        present[rec[0]] = 1;
    }

    // "dd:" prefix plus at most the whole variant payload hexed per row.
    HexOut ho; ho.out = begin_hex_out(num_rows, (uint32_t)(num_rows * (4u + d.data_size * 2u) + 16u));
    if (!ho.out) return NULL;
    ho.cursor = 0;

    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t idx = (d.type & COL_IS_CONST) ? 0u : i;
        uint8_t discr = cb.base[d.null_offset + idx];
        if (discr == 255u) {
            hex_row(&ho, i, NULL, 0, 1);
            continue;
        }
        if (!present[discr]) return NULL;
        uint32_t off; memcpy(&off, cb.base + d.offsets_offset + (uint64_t)idx * 4u, 4);
        const ColDesc * s = &subs[discr];
        const uint8_t * src;
        uint32_t n;
        if ((s->type & COL_TAG_MASK) == COL_BYTES) {
            uint64_t start, end;
            memcpy(&start, cb.base + s->offsets_offset + (uint64_t)off * 8u, 8);
            memcpy(&end, cb.base + s->offsets_offset + (uint64_t)(off + 1u) * 8u, 8);
            src = cb.base + s->data_offset + start;
            n = (uint32_t)(end - start);
        } else {
            if (s->null_offset == 0u) return NULL;
            n = (uint32_t)(s->data_size / s->null_offset);
            src = cb.base + s->data_offset + (uint64_t)off * n;
        }
        uint8_t * p = ho.out->data;
        ColDesc od; memcpy(&od, p + HEADER_BYTES, DESC_BYTES);
        p[od.data_offset + ho.cursor++] = (uint8_t)hex_digits[discr >> 4];
        p[od.data_offset + ho.cursor++] = (uint8_t)hex_digits[discr & 0xFu];
        p[od.data_offset + ho.cursor++] = ':';
        hex_row(&ho, i, src, n, 0);
    }
    finish_hex(&ho);
    return ho.out;
}
