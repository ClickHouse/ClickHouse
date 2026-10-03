/* A guest that reports the size of the batch it was called with, over a `Buffers` wire.
   Kept apart from `buffered_abi.c` because it is linked with an explicit initial linear memory:
   the byte budget the host derives from that number has to be exact for the test to name the
   batch boundary. */

#include <stdint.h>
#include <stddef.h>

typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

#define HEAP_SIZE (1 << 18)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 64
static Span spans[MAX_SPANS];
static uint32_t span_pos = 0;
static uint32_t live_spans = 0;

Span * clickhouse_create_buffer(uint32_t size) {
    uint32_t aligned_size = (size + 15u) & ~15u;
    if (span_pos >= MAX_SPANS) return NULL;
    if (heap_pos + aligned_size > HEAP_SIZE) return NULL;
    Span * span = &spans[span_pos++];
    span->data = &heap[heap_pos];
    span->size = size;
    heap_pos += aligned_size;
    ++live_spans;
    return span;
}

/* A bump allocator has nothing to reclaim for one buffer, so the arena is reset once the host has
   released every buffer of the call. Compartments are pooled and a query splits a block into many
   calls, so an arena that only ever grew would run out. */
void clickhouse_destroy_buffer(Span * data) {
    (void)data;
    if (live_spans > 0 && --live_spans == 0) {
        heap_pos = 0;
        span_pos = 0;
    }
}

static void write_le64(uint8_t * data, uint64_t value) {
    for (uint32_t i = 0; i < 8; ++i)
        data[i] = (uint8_t)(value >> (i * 8));
}

/* Writes, once per row, how many rows the call carried. Input and output are `Buffers` blocks of a
   single `UInt64` column: column count, row count, column size, then the values. The input is not
   read - the row count the host passes is the whole observable. */
Span * get_block_size_buffers(Span * input, uint32_t n) {
    (void)input;

    Span * res = clickhouse_create_buffer(24 + 8 * n);
    if (!res) return NULL;

    uint8_t * out = res->data;
    write_le64(out, 1);
    write_le64(out + 8, n);
    write_le64(out + 16, (uint64_t)n * 8);
    for (uint32_t i = 0; i < n; ++i)
        write_le64(out + 24 + i * 8, n);
    return res;
}
