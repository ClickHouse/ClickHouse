#include <stdint.h>

#define WASM_EXPORT(name) \
  __attribute__((export_name(#name))) \
  name


int64_t WASM_EXPORT(mobius)(uint64_t n) {
    if (n == 1) return 1;

    uint64_t prime_factors = 0;
    uint64_t temp = n;

    for (uint64_t i = 2; i * i <= n; i++) {
        if (temp % i == 0) {
            temp /= i;
            prime_factors++;
            if (temp % i == 0) return 0;  // Square factor
        }
    }

    if (temp > 1) prime_factors++;
    return (prime_factors & 1) ? -1 : 1;
}
