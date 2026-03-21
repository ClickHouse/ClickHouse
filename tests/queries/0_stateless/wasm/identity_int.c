/* identity_int.c
 * WASM UDF identity functions covering all ABIs and serialization formats.
 *
 * Exports:
 *   identity_raw            - ROW_DIRECT  Int32 identity  (i32 → i32)
 *   add                     - ROW_DIRECT  Int32 addition   (i32, i32 → i32)
 *   identity_msgpack_i32    - BUFFERED_V1 Int32 identity via MsgPack
 *   identity_msgpack_i64    - BUFFERED_V1 Int64 identity via MsgPack
 *   identity_tsv_i32        - BUFFERED_V1 Int32 identity via TSV (byte passthrough)
 *
 * Build via build.mk:
 *   make -f build.mk
 */

#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint8_t * data;
    uint32_t size;
} Span;

/* ---- Bump allocator ---- */
#define HEAP_SIZE (1 << 20)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 64
static Span spans[MAX_SPANS];
static uint32_t span_count = 0;

Span * clickhouse_create_buffer(uint32_t size)
{
    uint32_t aligned_size = (size + 15u) & ~15u;
    if (span_count >= MAX_SPANS || heap_pos + aligned_size > HEAP_SIZE) return NULL;
    Span * s = &spans[span_count++];
    s->data = &heap[heap_pos];
    s->size = size;
    heap_pos += aligned_size;
    return s;
}

void clickhouse_destroy_buffer(Span * s) { (void)s; }

/* ---- ROW_DIRECT UDFs ---- */
int32_t identity_raw(int32_t x) { return x; }
int32_t add(int32_t a, int32_t b) { return a + b; }

/* ---- MsgPack integer decoder: int32 ---- */
static int32_t read_msgpack_i32(const uint8_t **p)
{
    uint8_t b = *(*p)++;
    if (b <= 0x7f) return (int32_t)b;                                          /* positive fixint */
    if (b >= 0xe0) return (int32_t)(int8_t)b;                                  /* negative fixint */
    if (b == 0xcc) return (int32_t)(uint32_t)*(*p)++;                          /* uint8  */
    if (b == 0xcd) { uint16_t v = (uint16_t)(((uint16_t)(*p)[0]<<8)|(*p)[1]); *p+=2; return (int32_t)(uint32_t)v; } /* uint16 */
    if (b == 0xce) { uint32_t v = ((uint32_t)(*p)[0]<<24)|((uint32_t)(*p)[1]<<16)|((uint32_t)(*p)[2]<<8)|(*p)[3]; *p+=4; return (int32_t)v; } /* uint32 */
    if (b == 0xd0) return (int32_t)(int8_t)*(*p)++;                            /* int8   */
    if (b == 0xd1) { int16_t v = (int16_t)(((uint16_t)(*p)[0]<<8)|(*p)[1]);  *p+=2; return (int32_t)v; } /* int16  */
    if (b == 0xd2) { uint32_t v = ((uint32_t)(*p)[0]<<24)|((uint32_t)(*p)[1]<<16)|((uint32_t)(*p)[2]<<8)|(*p)[3]; *p+=4; return (int32_t)v; } /* int32  */
    return 0;
}

/* ---- MsgPack int32 encoder (always 5 bytes: 0xd2 + big-endian) ---- */
static void write_msgpack_i32(uint8_t **p, int32_t v)
{
    uint32_t u = (uint32_t)v;
    *(*p)++ = 0xd2;
    *(*p)++ = (uint8_t)(u >> 24);
    *(*p)++ = (uint8_t)(u >> 16);
    *(*p)++ = (uint8_t)(u >> 8);
    *(*p)++ = (uint8_t)u;
}

/* ---- MsgPack integer decoder: int64 ---- */
static int64_t read_msgpack_i64(const uint8_t **p)
{
    uint8_t b = *(*p)++;
    if (b <= 0x7f) return (int64_t)b;                                          /* positive fixint */
    if (b >= 0xe0) return (int64_t)(int8_t)b;                                  /* negative fixint */
    if (b == 0xcc) return (int64_t)(uint64_t)*(*p)++;                          /* uint8  */
    if (b == 0xcd) { uint16_t v = (uint16_t)(((uint16_t)(*p)[0]<<8)|(*p)[1]); *p+=2; return (int64_t)(uint64_t)v; } /* uint16 */
    if (b == 0xce) { uint32_t v = ((uint32_t)(*p)[0]<<24)|((uint32_t)(*p)[1]<<16)|((uint32_t)(*p)[2]<<8)|(*p)[3]; *p+=4; return (int64_t)(uint64_t)v; } /* uint32 */
    if (b == 0xcf) { uint64_t v = ((uint64_t)(*p)[0]<<56)|((uint64_t)(*p)[1]<<48)|((uint64_t)(*p)[2]<<40)|((uint64_t)(*p)[3]<<32)
                                 |((uint64_t)(*p)[4]<<24)|((uint64_t)(*p)[5]<<16)|((uint64_t)(*p)[6]<<8)|(*p)[7]; *p+=8; return (int64_t)v; } /* uint64 */
    if (b == 0xd0) return (int64_t)(int8_t)*(*p)++;                            /* int8   */
    if (b == 0xd1) { int16_t v = (int16_t)(((uint16_t)(*p)[0]<<8)|(*p)[1]);  *p+=2; return (int64_t)v; } /* int16  */
    if (b == 0xd2) { uint32_t v = ((uint32_t)(*p)[0]<<24)|((uint32_t)(*p)[1]<<16)|((uint32_t)(*p)[2]<<8)|(*p)[3]; *p+=4; return (int64_t)(int32_t)v; } /* int32  */
    if (b == 0xd3) { uint64_t v = ((uint64_t)(*p)[0]<<56)|((uint64_t)(*p)[1]<<48)|((uint64_t)(*p)[2]<<40)|((uint64_t)(*p)[3]<<32)
                                 |((uint64_t)(*p)[4]<<24)|((uint64_t)(*p)[5]<<16)|((uint64_t)(*p)[6]<<8)|(*p)[7]; *p+=8; return (int64_t)v; } /* int64  */
    return 0;
}

/* ---- MsgPack int64 encoder (always 9 bytes: 0xd3 + big-endian) ---- */
static void write_msgpack_i64(uint8_t **p, int64_t v)
{
    uint64_t u = (uint64_t)v;
    *(*p)++ = 0xd3;
    *(*p)++ = (uint8_t)(u >> 56);
    *(*p)++ = (uint8_t)(u >> 48);
    *(*p)++ = (uint8_t)(u >> 40);
    *(*p)++ = (uint8_t)(u >> 32);
    *(*p)++ = (uint8_t)(u >> 24);
    *(*p)++ = (uint8_t)(u >> 16);
    *(*p)++ = (uint8_t)(u >> 8);
    *(*p)++ = (uint8_t)u;
}

/* ---- BUFFERED_V1 MsgPack Int32 ---- */
Span * identity_msgpack_i32(Span * input, uint32_t num_rows)
{
    Span * out = clickhouse_create_buffer(num_rows * 5);
    if (!out) return NULL;

    const uint8_t * in_p  = input->data;
    uint8_t       * out_p = out->data;

    for (uint32_t i = 0; i < num_rows; i++)
        write_msgpack_i32(&out_p, read_msgpack_i32(&in_p));

    out->size = (uint32_t)(out_p - out->data);
    return out;
}

/* ---- BUFFERED_V1 MsgPack Int64 ---- */
Span * identity_msgpack_i64(Span * input, uint32_t num_rows)
{
    Span * out = clickhouse_create_buffer(num_rows * 9);
    if (!out) return NULL;

    const uint8_t * in_p  = input->data;
    uint8_t       * out_p = out->data;

    for (uint32_t i = 0; i < num_rows; i++)
        write_msgpack_i64(&out_p, read_msgpack_i64(&in_p));

    out->size = (uint32_t)(out_p - out->data);
    return out;
}

/* ---- BUFFERED_V1 TSV Int32 (byte passthrough — text round-trip is exact) ---- */
Span * identity_tsv_i32(Span * input, uint32_t num_rows)
{
    (void)num_rows;
    Span * out = clickhouse_create_buffer(input->size);
    if (!out) return NULL;
    for (uint32_t i = 0; i < input->size; i++)
        out->data[i] = input->data[i];
    out->size = input->size;
    return out;
}
