#include <stdint.h>

extern uint64_t clickhouse_server_version();
extern void clickhouse_log(const char * message, uint32_t length);
extern void clickhouse_throw(const char * message, uint32_t length);
extern void clickhouse_random(void * data, uint32_t size);

uint32_t int_to_str(uint64_t n, char * buf) {
    uint32_t len = 0;
    uint64_t t = n;
    while (t > 0) {
        t /= 10;
        len++;
    }
    for (uint32_t i = 0; i < len; i++) {
        buf[len - i - 1] = '0' + n % 10;
        n /= 10;
    }
    return len;
}

uint32_t copy_str(const char * src, char * dst, uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        dst[i] = src[i];
    }
    return len;
}

uint32_t test_func(uint32_t terminate) {
    uint64_t version = clickhouse_server_version();

    char buf[64];
    char * p = buf;
    p += copy_str("Hello, ClickHouse ", p, 18);
    p += int_to_str(version, p);
    p += copy_str("!", p, 1);
    clickhouse_log(buf, p - buf);

    if (terminate) {
        clickhouse_throw("Goodbye, ClickHouse!", 20);
    }
    return 0;
}

uint32_t test_random(uint32_t arg) {
    uint32_t value;
    clickhouse_random(&value, sizeof(value));

    char buf[64];
    char * p = buf;
    p += copy_str("test_random(", p, 12);
    p += int_to_str(arg, p);
    p += copy_str(") = ", p, 4);
    p += int_to_str(value, p);
    clickhouse_log(buf, p - buf);

    return value;
}
