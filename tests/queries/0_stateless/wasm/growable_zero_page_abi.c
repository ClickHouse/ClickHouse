#include <stdint.h>

/*
 * Guest for the zero-initial-page buffered-ABI regression test
 * (05123_wasm_abi_growable_zero_page_memory_split.sh): its linear memory is linked with
 * `memory 0 N`, so the module starts with no pages at all and grows on demand. The splitter has a
 * dedicated path for that shape - the reachable ceiling is the declared maximum, not the initial
 * size - and this guest lets a test observe from SQL that batching still happens there.
 *
 * Everything lives in linear memory the guest grows itself: with zero initial pages there is no
 * place for static data, a shadow stack or address-taken locals, so the allocator keeps its bump
 * pointer and live-buffer count at fixed addresses and the formatter writes its digits straight
 * into the output buffer.
 */

typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

/* Address zero is the guest's null pointer, and dereferencing it is undefined behaviour the
   compiler is free to fold away, so the allocator's own words start past it. */
#define BUMP_ADDR 8u
#define LIVE_ADDR 12u
#define HEAP_START 16u
#define PAGE_SIZE 65536u

static inline uint32_t load(uint32_t address) { return *(uint32_t *)(uintptr_t)address; }
static inline void store(uint32_t address, uint32_t value) { *(uint32_t *)(uintptr_t)address = value; }

Span * clickhouse_create_buffer(uint32_t size) {
    if (__builtin_wasm_memory_size(0) == 0) {
        if (__builtin_wasm_memory_grow(0, 1) == (uint32_t)-1) return 0;
    }
    /* Address zero is the guest's null, so the heap starts past the two allocator words. A fresh
       page reads as zero, which is how the first call recognizes an uninitialized bump pointer. */
    if (load(BUMP_ADDR) == 0u)
        store(BUMP_ADDR, HEAP_START);
    uint32_t aligned_size = (size + 15u) & ~15u;
    if (aligned_size < size) return 0;
    uint32_t bump = load(BUMP_ADDR);
    uint32_t end = bump + 16u + aligned_size;
    uint32_t have = __builtin_wasm_memory_size(0) * PAGE_SIZE;
    if (end > have) {
        uint32_t pages = (end - have + PAGE_SIZE - 1u) / PAGE_SIZE;
        if (__builtin_wasm_memory_grow(0, pages) == (uint32_t)-1) return 0;
    }
    Span * span = (Span *)(uintptr_t)bump;
    span->data = (uint8_t *)(uintptr_t)(bump + 16u);
    span->size = size;
    store(BUMP_ADDR, end);
    store(LIVE_ADDR, load(LIVE_ADDR) + 1u);
    return span;
}

void clickhouse_destroy_buffer(Span * span) {
    (void)span;
    /* Bump allocator: reclaim everything once the host has released both the input and the result
       buffer of a call, so consecutive batches on the same compartment don't exhaust the heap. */
    uint32_t live = load(LIVE_ADDR);
    if (live > 0u) {
        store(LIVE_ADDR, live - 1u);
        if (live == 1u)
            store(BUMP_ADDR, HEAP_START);
    }
}

/* (x String) -> UInt32 over CSV: ignores the input payload - its allocation already proves the
   batch fit - and returns the batch's row count once per row. */
Span * batch_row_count(Span * input, uint32_t num_rows) {
    (void)input;
    uint32_t len = 0;
    for (uint32_t t = num_rows; t > 0u; t /= 10u) len++;
    if (len == 0u) len = 1u;
    Span * out = clickhouse_create_buffer(num_rows * (len + 1u));
    if (out == 0) return 0;
    uint8_t * pos = out->data;
    for (uint32_t row = 0; row < num_rows; row++) {
        uint32_t value = num_rows;
        for (uint32_t i = 0; i < len; i++) {
            uint32_t divisor = 1u;
            for (uint32_t d = 0; d < len - i - 1u; d++) divisor *= 10u;
            *pos++ = (uint8_t)('0' + (value / divisor) % 10u);
        }
        *pos++ = '\n';
    }
    return out;
}

