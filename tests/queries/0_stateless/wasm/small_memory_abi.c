#include <stdint.h>
#include <stddef.h>

/*
 * Guest for two properties the row-count guests cannot show:
 *
 *  - `input_size_json` reports the size of the serialized input buffer it was handed, so a test
 *    can tell what the host actually put on the wire for a row - a declared `Nullable` parameter
 *    receiving `null` versus a non-nullable one receiving the declared default, for instance.
 *  - the module declares an explicit maximum for its linear memory (`--max-memory` at link time,
 *    see build.mk), which is smaller than any sensible `webassembly_udf_max_memory`, so it
 *    exercises the module-declared half of `WasmCompartment::getMaxLinearMemorySize`.
 */

typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

#define HEAP_SIZE (1 << 15)
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

/* `RETURNS Array(UInt32)` over JSONEachRow: returns the size of the serialized input buffer,
   once per row. `Array` cannot live inside `Nullable`, so the function opts out of the default
   null handling and the guest sees whatever the ABI decided to write for a null row. */
Span * input_size_json(Span * input, uint32_t num_rows) {
    uint32_t input_size = input == NULL ? 0 : input->size;
    uint8_t digits[10];
    uint32_t len = write_u32(input_size, digits);
    Span * out = clickhouse_create_buffer(num_rows * (14 + len));
    if (out == NULL) return NULL;
    static const char prefix[] = "{\"result\":[";
    uint8_t * pos = out->data;
    for (uint32_t row = 0; row < num_rows; row++) {
        for (uint32_t i = 0; prefix[i]; i++)
            *pos++ = prefix[i];
        for (uint32_t i = 0; i < len; i++)
            *pos++ = digits[i];
        *pos++ = ']';
        *pos++ = '}';
        *pos++ = '\n';
    }
    out->size = (uint32_t)(pos - out->data);
    return out;
}
