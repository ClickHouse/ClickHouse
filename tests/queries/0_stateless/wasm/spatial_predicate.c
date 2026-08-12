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
 * `point_in_rect` is a genuine (if trivial) spatial predicate: axis-aligned
 * rectangle containment, i.e. the same relation as the `bbox_op_rcontains`
 * bbox shortcut used by `st_within` in a real GEOS-backed WASM UDF geometry
 * library, just operating directly on a `Point` and a
 * `Tuple(Float64, Float64, Float64, Float64)` bbox instead of on parsed WKB
 * geometries. It is used as a non-`pointInPolygon` reference for the
 * `spatial_bbox` index's `isSpatialPredicate()`-based extraction, to show
 * pruning also works for a predicate whose semantics are unrelated to
 * `pointInPolygon`'s ring/hole assembly.
 *
 * Exports:
 *   always_true   - BUFFERED_V1 UInt8, ignores input, writes "1\n" per row.
 *   point_in_rect - BUFFERED_V1 UInt8 via RowBinary: (Point, Tuple(Float64 x 4)) -> UInt8.
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

static double read_f64le(const uint8_t * p)
{
    uint64_t u = 0;
    for (int i = 7; i >= 0; i--)
        u = (u << 8) | p[i];
    double d;
    __builtin_memcpy(&d, &u, sizeof(d));
    return d;
}

/* RowBinary row layout: Point (x, y) then Tuple(min_x, min_y, max_x, max_y), 6 LE float64s. */
Span * point_in_rect(Span * input, uint32_t n)
{
    Span * res = clickhouse_create_buffer(n);
    if (!res) return NULL;
    const uint8_t * p = input->data;
    for (uint32_t i = 0; i < n; i++)
    {
        double x = read_f64le(p);      p += 8;
        double y = read_f64le(p);      p += 8;
        double min_x = read_f64le(p);  p += 8;
        double min_y = read_f64le(p);  p += 8;
        double max_x = read_f64le(p);  p += 8;
        double max_y = read_f64le(p);  p += 8;
        res->data[i] = (x >= min_x && x <= max_x && y >= min_y && y <= max_y) ? 1 : 0;
    }
    res->size = n;
    return res;
}
