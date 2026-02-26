#include <stdint.h>
#include <stddef.h>

#define WASM_PAGE_SIZE (1 << 16)

typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

#define HEAP_SIZE (1 << 26)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 4096
static Span spans[MAX_SPANS];
static uint32_t span_pos = 0;

extern void clickhouse_log(const char * message, uint32_t length);

Span * clickhouse_create_buffer(uint32_t size) {
    if (span_pos >= MAX_SPANS) return NULL;
    if (heap_pos + size > HEAP_SIZE) return NULL;
    Span * span = &spans[span_pos++];
    span->data = &heap[heap_pos];
    span->size = size;
    heap_pos += (size + 15) & ~15u;
    return span;
}

void clickhouse_destroy_buffer(Span * data) {
    clickhouse_log("XXXX Buffer destroyed", 20);
    (void)data;
}

/* FNV-1a 64-bit hash over a byte range */
static uint64_t fnv1a_64(const uint8_t * data, uint32_t size) {
    uint64_t hash = 14695981039346656037ULL;
    for (uint32_t i = 0; i < size; i++) {
        hash ^= (uint64_t)data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static uint32_t write_u64(uint64_t val, char * buf) {
    if (val == 0) {
        buf[0] = '0';
        return 1;
    }

    uint32_t len = 0;
    uint64_t t = val;
    while (t > 0) {
        t /= 10;
        len++;
    }
    for (uint32_t i = 0; i < len; i++) {
        buf[len - i - 1] = '0' + val % 10;
        val /= 10;
    }
    return len;
}

/* Digest rows separated by '\n' (CSV or TSV serialization format).
   Output: one UInt64 decimal per row followed by '\n'.
   No deep field parsing — just hashes the raw bytes of each line. */
Span * digest_newline_rows(Span * span, uint32_t n) {
    /* UInt64 max is 20 digits, plus '\n' = 21 bytes per row */
    Span * res = clickhouse_create_buffer(n * 21);
    if (!res) return NULL;

    const uint8_t * p = span->data;
    const uint8_t * end = span->data + span->size;
    char * out = (char *)res->data;
    uint32_t pos = 0;

    for (uint32_t i = 0; i < n; i++) {
        const uint8_t * row_start = p;
        while (p < end && *p != '\n') p++;
        uint64_t h = fnv1a_64(row_start, (uint32_t)(p - row_start));
        pos += write_u64(h, out + pos);
        out[pos++] = '\n';
        if (p < end) p++; /* skip '\n' */
    }

    res->size = pos;
    return res;
}

/* Digest for JSONEachRow serialization_format.
   Shallow check: each row must start with '{' and end with '}'.
   Output rows: {"result":[<hash>,<content_len>]}\n per row — an array of two
   UInt64 values: FNV-1a hash of the row bytes and the content byte length.
   This exercises array return-type parsing on the ClickHouse side. */
Span * digest_json_rows(Span * span, uint32_t n) {
    /* {"result":[,]}\n = 14 chars, two UInt64 max 20 digits each → 54 bytes max */
    Span * res = clickhouse_create_buffer(n * 54);
    if (!res) return NULL;

    const uint8_t * p = span->data;
    const uint8_t * end = span->data + span->size;
    char * out = (char *)res->data;
    uint32_t pos = 0;

    static const char prefix[] = "{\"result\":[";

    for (uint32_t i = 0; i < n; i++) {
        const uint8_t * row_start = p;
        while (p < end && *p != '\n') p++;
        uint32_t row_len = (uint32_t)(p - row_start);

        /* Strip trailing '\r' for the structural check only */
        uint32_t content_len = row_len;
        while (content_len > 0 && row_start[content_len - 1] == '\r') content_len--;

        /* Shallow JSON row validation: must start with '{' and end with '}' */
        uint64_t h = 0;
        if (content_len >= 2 && row_start[0] == '{' && row_start[content_len - 1] == '}')
            h = fnv1a_64(row_start, row_len);

        for (uint32_t j = 0; prefix[j]; j++) out[pos++] = prefix[j];
        pos += write_u64(h, out + pos);
        out[pos++] = ',';
        pos += write_u64((uint64_t)content_len, out + pos);
        out[pos++] = ']';
        out[pos++] = '}';
        out[pos++] = '\n';

        if (p < end) p++; /* skip '\n' */
    }

    res->size = pos;
    return res;
}

Span * always_returns_ten_rows(Span * span, uint32_t n) {
    Span * res = clickhouse_create_buffer(10 * 21);
    if (!res) return NULL;
    char * buf = (char *)res->data;
    uint32_t pos = 0;
    for (uint32_t i = 0; i < 10; i++) {
        pos += write_u64(10, buf + pos);
        buf[pos++] = '\n';
    }
    res->size = pos;
    return res;
}

Span * get_block_size(Span * span, uint32_t n) {
    Span * res = clickhouse_create_buffer(n * 21);
    if (!res) return NULL;
    char * buf = (char *)res->data;
    uint32_t pos = 0;
    for (uint32_t i = 0; i < n; i++) {
        pos += write_u64(n, buf + pos);
        buf[pos++] = '\n';
    }
    res->size = pos;
    return res;
}
