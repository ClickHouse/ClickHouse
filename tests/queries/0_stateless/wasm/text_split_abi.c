#include <stdint.h>
#include <stddef.h>

/*
 * Guest for the buffered-ABI dynamic splitter regression test
 * (04510_wasm_abi_buffered_text_format_split.sh): the heap is deliberately small,
 * so a text-serialized batch whose wire size the splitter underestimates cannot be
 * allocated, while correctly split batches fit comfortably. Each call returns, for
 * every input row, the number of rows in the batch that the row arrived in, letting
 * the test observe from SQL that dynamic splitting actually happened.
 */

typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

#define HEAP_SIZE (1 << 20)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 64
static Span spans[MAX_SPANS];
static uint32_t span_pos = 0;
static uint32_t live_buffers = 0;

Span * clickhouse_create_buffer(uint32_t size) {
    uint32_t aligned_size = (size + 15u) & ~15u;
    if (span_pos >= MAX_SPANS) return NULL;
    if (aligned_size < size || aligned_size > HEAP_SIZE - heap_pos) return NULL;
    Span * span = &spans[span_pos++];
    span->data = &heap[heap_pos];
    span->size = size;
    heap_pos += aligned_size;
    live_buffers++;
    return span;
}

void clickhouse_destroy_buffer(Span * span) {
    (void)span;
    /* Bump allocator: reclaim everything once the host has released both the input
       and the result buffer of a call, so consecutive batches executed on the same
       compartment don't exhaust the heap. */
    if (live_buffers > 0 && --live_buffers == 0) {
        heap_pos = 0;
        span_pos = 0;
    }
}

static uint32_t write_u32(uint32_t val, uint8_t * buf) {
    if (val == 0) {
        buf[0] = '0';
        return 1;
    }
    uint32_t len = 0;
    for (uint32_t t = val; t > 0; t /= 10) len++;
    for (uint32_t i = 0; i < len; i++) {
        buf[len - i - 1] = '0' + val % 10;
        val /= 10;
    }
    return len;
}

/* (x Int8) -> UInt32 over CSV: ignores the input payload (its allocation already
   proves the batch fit), returns the batch's row count once per row. */
Span * batch_row_count(Span * input, uint32_t num_rows) {
    (void)input;
    uint8_t digits[10];
    uint32_t len = write_u32(num_rows, digits);
    Span * out = clickhouse_create_buffer(num_rows * (len + 1));
    if (out == NULL) return NULL;
    uint8_t * pos = out->data;
    for (uint32_t row = 0; row < num_rows; row++) {
        for (uint32_t i = 0; i < len; i++)
            *pos++ = digits[i];
        *pos++ = '\n';
    }
    return out;
}
