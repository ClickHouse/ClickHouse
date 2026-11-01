#include <stdint.h>


typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

static char data[sizeof(Span)] = { 0 };

#define WASM_PAGE_SIZE (1 << 16)

Span * clickhouse_create_buffer(uint32_t size) {
    Span * span = (Span *)data;
    uint8_t * end  = (uint8_t *)(__builtin_wasm_memory_size(0) * WASM_PAGE_SIZE);
    span->data = end;
    span->size = size;
    return span;
}

void clickhouse_destroy_buffer(Span * ptr) {
}

Span * test_func(Span * data, uint32_t num_rows) {
    return data;
}
