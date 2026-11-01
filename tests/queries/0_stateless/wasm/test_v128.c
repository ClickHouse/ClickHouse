#include <stdint.h>
#include <wasm_simd128.h>

v128_t concatInts(uint64_t high, uint64_t low) {
    return wasm_i64x2_make(low, high);
}
