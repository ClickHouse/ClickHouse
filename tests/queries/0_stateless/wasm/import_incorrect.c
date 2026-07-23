
#include <stdint.h>

extern void clickhouse_log(const char * message);

uint32_t test_func(uint32_t a) {
    clickhouse_log("Hello, ClickHouse!");
    return 1;
}



