#include <stdint.h>

#define WASM_PAGE_SIZE (1 << 16)

uint32_t huge_allocate(uint32_t s) {
    uint32_t i = 0;
    while (1) {
        if (__builtin_wasm_memory_grow(0, s) == -1) {
            break;
        }
        i++;
    }
    return i;
}

uint32_t infinite_loop(uint32_t s) {
    if (s == 0) {
        return 0;
    }

    volatile uint32_t i = 0;
    while (1) {
        i++;
    }
    return i;
}

uint32_t fib(uint32_t n) {
    if (n == 0) {
        __builtin_trap();
    }
    if (n <= 2) {
        return n;
    }
    return fib(n - 1) + fib(n - 2);
}

uint32_t write_out_of_bounds(uint32_t n) {
    uint8_t * end  = (uint8_t *)(__builtin_wasm_memory_size(0) * WASM_PAGE_SIZE);
    for (uint32_t i = 0; i < n; i++) {
        *end = 1;
        end++;
    }
    return 0;
}

uint32_t read_out_of_bounds(uint32_t n) {
    uint32_t sum = 0;
    uint8_t * end  = (uint8_t *)(__builtin_wasm_memory_size(0) * WASM_PAGE_SIZE);
    for (uint32_t i = 0; i < n; i++) {
        sum += *end;
        end++;
    }
    return sum;
}

