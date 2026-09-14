/* spatial_predicate.c
 * WASM UDF used to regression-test the `is_spatial_predicate = 1` fail-closed
 * path in GeoParquet row-group pruning (Processors/Formats/Impl/Parquet/GeoFilter.cpp).
 *
 * `always_true` ignores its input entirely and returns UInt8 1 for every row.
 * It is deliberately content-agnostic: the test that uses it only cares about
 * the *shape* of the predicate (two non-constant column arguments plus a
 * constant), not about real geometric computation, so the guest never needs
 * to decode the buffered input at all.
 *
 * Exports:
 *   always_true - BUFFERED_V1 UInt8, ignores input, writes "1\n" per row.
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

#define HEAP_SIZE (1 << 16)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 16
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

/* Ignores the serialized input entirely; writes UInt8 '1' + '\n' per row. */
Span * always_true(Span * input, uint32_t n)
{
    (void)input;
    Span * res = clickhouse_create_buffer(n * 2);
    if (!res) return NULL;
    for (uint32_t i = 0; i < n; i++)
    {
        res->data[i * 2] = '1';
        res->data[i * 2 + 1] = '\n';
    }
    res->size = n * 2;
    return res;
}
