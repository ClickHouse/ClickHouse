
#include <stdint.h>

extern uint32_t unknown_import2(uint32_t);

uint32_t test_func(uint32_t a) {
    return unknown_import2(a) + 1;
}
