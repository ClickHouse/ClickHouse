#include <stdint.h>

char * clickhouse_create_buffer(uint32_t size) {
    return 0;
}

/// Expected clickhouse_destroy_buffer(char * ptr)
void clickhouse_destroy_buffer(char * ptr, uint32_t size) {}

char * test_func(char * data, uint32_t num_rows) {
    return data;
}
