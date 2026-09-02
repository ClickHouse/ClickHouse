#include <stdint.h>
#include <stddef.h>

#define WASM_PAGE_SIZE (1 << 16)

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
    (void)data;
}

Span * test_func(Span * span, uint32_t n) {
    Span * res = clickhouse_create_buffer(n * 2);
    char * input = (char *)span->data;
    char * output = (char *)res->data;
    for (uint32_t row_num = 0; row_num < n; row_num++) {
        *output = *input;
        output++;
        *output = '\n';
        output++;
        while (*input != '\n') {
            input++;
        }
        input++;
    }
    return res;
}

uint8_t * returns_out_of_bounds(uint8_t * data, uint32_t n) {
    uint8_t * ptr = (uint8_t *)(__builtin_wasm_memory_size(0) * WASM_PAGE_SIZE);
    ptr -= sizeof(uint32_t);
    ((uint32_t *)ptr)[0] = 42;
    return ptr;
}

Span * returns_out_of_bounds2(Span * data, uint32_t n) {
    Span * ptr = clickhouse_create_buffer(0);
    ptr->size = 42;
    ptr->data = (uint8_t *)(__builtin_wasm_memory_size(0) * WASM_PAGE_SIZE);
    return ptr;
}
