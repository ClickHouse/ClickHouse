"""Shared code of the data lake corpus generator: canonical datasets, normalization of
readback data into the expected JSON Lines, and the manifest.

Runs in the orchestrator environment (see `requirements.txt`). Writer scripts under
`writers/` run in their own pinned environments and only exchange Arrow IPC files and
JSON job descriptions with this module, so they do not import it.
"""

import datetime
import decimal
import hashlib
import io
import json
import math
import random
import uuid as uuidlib

import pyarrow as pa
import pyarrow.ipc as ipc
import zstandard

decimal.getcontext().prec = 100

EPOCH = datetime.datetime(1970, 1, 1)
EPOCH_DATE = datetime.date(1970, 1, 1)

# ---------------------------------------------------------------------------
# Canonical datasets
# ---------------------------------------------------------------------------

WORDS = [
    "", "a", "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
    "lambda", "mu", "nu", "xi", "omicron", "pi", "rho", "sigma", "tau", "upsilon", "phi", "chi",
    "psi", "omega", "Ünïcødé", "日本語テキスト", "emoji 🎉", "tab\there", "new\nline", "quote\"s",
    "back\\slash", "comma,semicolon;", "zzz", "Zebra", "mañana",
]


def _rng(name):
    return random.Random(int(hashlib.sha256(name.encode()).hexdigest()[:16], 16))


def _maybe_null(rng, value, p=0.15):
    return None if rng.random() < p else value


def _text(rng, lo=0, hi=40):
    n = rng.randint(lo, hi)
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-./"
    return "".join(rng.choice(alphabet) for _ in range(n))


def _ts(rng, lo_year=1900, hi_year=2200):
    lo = int((datetime.datetime(lo_year, 1, 1) - EPOCH).total_seconds())
    hi = int((datetime.datetime(hi_year, 1, 1) - EPOCH).total_seconds())
    return rng.randint(lo, hi)


def _int_extremes(bits, signed):
    if signed:
        lo, hi = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
        return [lo, hi, 0, -1, 1, lo + 1, hi - 1]
    hi = (1 << bits) - 1
    return [0, hi, 1, hi - 1, 1 << (bits - 1)]


def _int_column(rng, n, bits, signed, null_p=0.1):
    ext = _int_extremes(bits, signed)
    lo = -(1 << (bits - 1)) if signed else 0
    hi = (1 << (bits - 1)) - 1 if signed else (1 << bits) - 1
    out = []
    for i in range(n):
        if i < len(ext):
            out.append(ext[i])
        else:
            out.append(_maybe_null(rng, rng.randint(lo, hi) if rng.random() < 0.5 else rng.randint(-100 if signed else 0, 100), null_p))
    return out


def _float_values(rng, n, f32):
    special = [0.0, -0.0, 1.0, -1.0, 1.5e-30 if f32 else 1e-300, 3.0e38 if f32 else 1.7e308, float("inf"), float("-inf")]
    out = []
    for i in range(n):
        if i < len(special):
            out.append(special[i])
        else:
            v = rng.uniform(-1e6, 1e6) if rng.random() < 0.7 else rng.gauss(0, 1)
            out.append(_maybe_null(rng, v))
    return out


def _decimal(rng, precision, scale):
    digits = rng.randint(1, precision)
    unscaled = rng.randint(0, 10 ** digits - 1) * (1 if rng.random() < 0.5 else -1)
    return decimal.Decimal(unscaled).scaleb(-scale)


def ds_flat(n=120):
    """Flat table with every common primitive type, nulls, extremes."""
    rng = _rng("flat")
    cols = {}
    cols["id"] = pa.array(range(n), pa.int64())
    cols["b"] = pa.array([_maybe_null(rng, rng.random() < 0.5) for _ in range(n)], pa.bool_())
    for bits in (8, 16, 32, 64):
        cols[f"i{bits}"] = pa.array(_int_column(rng, n, bits, True), getattr(pa, f"int{bits}")())
        cols[f"u{bits}"] = pa.array(_int_column(rng, n, bits, False), getattr(pa, f"uint{bits}")())
    cols["f32"] = pa.array(_float_values(rng, n, True), pa.float32())
    cols["f64"] = pa.array(_float_values(rng, n, False), pa.float64())
    cols["s"] = pa.array([_maybe_null(rng, rng.choice(WORDS) if rng.random() < 0.5 else _text(rng, 0, 50)) for _ in range(n)], pa.string())
    cols["s_dict"] = pa.array([_maybe_null(rng, rng.choice(WORDS[:8])) for _ in range(n)], pa.string())
    cols["bin"] = pa.array([_maybe_null(rng, _text(rng, 0, 30).encode()) for _ in range(n)], pa.binary())
    cols["fixed"] = pa.array([_maybe_null(rng, _text(rng, 5, 5).encode()) for _ in range(n)], pa.binary(5))
    lo_d = (datetime.date(1900, 1, 1) - EPOCH_DATE).days
    hi_d = (datetime.date(2299, 12, 31) - EPOCH_DATE).days
    dates = [lo_d, hi_d, 0, -1] + [_maybe_null(rng, rng.randint(lo_d, hi_d)) for _ in range(n - 4)]
    cols["d"] = pa.array(dates, pa.int32()).cast(pa.date32())
    cols["ts_ms"] = pa.array([_maybe_null(rng, _ts(rng) * 1000 + rng.randint(0, 999)) for _ in range(n)], pa.int64()).cast(pa.timestamp("ms"))
    cols["ts_us"] = pa.array([_maybe_null(rng, _ts(rng) * 10**6 + rng.randint(0, 10**6 - 1)) for _ in range(n)], pa.int64()).cast(pa.timestamp("us"))
    cols["ts_us_utc"] = pa.array([_maybe_null(rng, _ts(rng) * 10**6 + rng.randint(0, 10**6 - 1)) for _ in range(n)], pa.int64()).cast(pa.timestamp("us", "UTC"))
    cols["ts_ns"] = pa.array([_maybe_null(rng, _ts(rng, 1678, 2261) * 10**9 + rng.randint(0, 10**9 - 1)) for _ in range(n)], pa.int64()).cast(pa.timestamp("ns"))
    cols["dec9"] = pa.array([_maybe_null(rng, _decimal(rng, 9, 2)) for _ in range(n)], pa.decimal128(9, 2))
    cols["dec18"] = pa.array([_maybe_null(rng, _decimal(rng, 18, 4)) for _ in range(n)], pa.decimal128(18, 4))
    cols["dec38"] = pa.array([_maybe_null(rng, _decimal(rng, 38, 10)) for _ in range(n)], pa.decimal128(38, 10))
    return pa.table(cols)


def ds_nested(n=100):
    """Lists, structs, maps, and their combinations, with nulls at every level."""
    rng = _rng("nested")

    def lst(f, lo=0, hi=5, p=0.15):
        if rng.random() < p:
            return None
        return [f() for _ in range(rng.randint(lo, hi))]

    def small_int():
        return _maybe_null(rng, rng.randint(-1000, 1000), 0.1)

    def word():
        return _maybe_null(rng, rng.choice(WORDS), 0.1)

    rows = {k: [] for k in ["id", "l_i32", "l_s", "ll_i64", "st", "m", "m_i_l", "l_st", "st_l", "l_m"]}
    for i in range(n):
        rows["id"].append(i)
        rows["l_i32"].append(lst(small_int))
        rows["l_s"].append(lst(word))
        rows["ll_i64"].append(lst(lambda: lst(lambda: _maybe_null(rng, rng.randint(-(1 << 63), (1 << 63) - 1), 0.1), 0, 3, 0.1), 0, 3))
        rows["st"].append(None if rng.random() < 0.1 else {"a": small_int(), "b": word(), "c": None if rng.random() < 0.1 else {"d": _maybe_null(rng, rng.uniform(-10, 10))}})
        if rng.random() < 0.15:
            rows["m"].append(None)
        else:
            keys = rng.sample(WORDS[1:], rng.randint(0, 4))
            rows["m"].append([(k, small_int()) for k in keys])
        if rng.random() < 0.15:
            rows["m_i_l"].append(None)
        else:
            keys = rng.sample(range(-50, 50), rng.randint(0, 3))
            rows["m_i_l"].append([(k, lst(word, 0, 3)) for k in keys])
        rows["l_st"].append(lst(lambda: None if rng.random() < 0.1 else {"x": small_int(), "y": word()}, 0, 4))
        rows["st_l"].append(None if rng.random() < 0.1 else {"arr": lst(small_int), "mm": [(k, word()) for k in rng.sample(WORDS[1:], rng.randint(0, 2))]})
        rows["l_m"].append(lst(lambda: [(k, small_int()) for k in rng.sample(WORDS[1:], rng.randint(0, 2))], 0, 3))
    m_si = pa.map_(pa.string(), pa.int32())
    schema = pa.schema([
        ("id", pa.int64()),
        ("l_i32", pa.list_(pa.int32())),
        ("l_s", pa.list_(pa.string())),
        ("ll_i64", pa.list_(pa.list_(pa.int64()))),
        ("st", pa.struct([("a", pa.int32()), ("b", pa.string()), ("c", pa.struct([("d", pa.float64())]))])),
        ("m", m_si),
        ("m_i_l", pa.map_(pa.int32(), pa.list_(pa.string()))),
        ("l_st", pa.list_(pa.struct([("x", pa.int32()), ("y", pa.string())]))),
        ("st_l", pa.struct([("arr", pa.list_(pa.int32())), ("mm", pa.map_(pa.string(), pa.string()))])),
        ("l_m", pa.list_(m_si)),
    ])
    return pa.table(rows, schema=schema)


def ds_strings(n=400):
    """Strings for DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY / dictionary fallback."""
    rng = _rng("strings")
    hosts = ["https://clickhouse.com/docs/", "https://github.com/ClickHouse/ClickHouse/pull/", "s3://bucket/path/to/data/", "file:///var/lib/"]
    prefixed = sorted(rng.choice(hosts) + _text(rng, 0, 25) for _ in range(n))
    cols = {
        "id": pa.array(range(n), pa.int64()),
        "s": pa.array([_maybe_null(rng, rng.choice(WORDS) if rng.random() < 0.3 else _text(rng, 0, 30)) for _ in range(n)], pa.string()),
        "s_prefix": pa.array(prefixed, pa.string()),
        "s_low_card": pa.array([rng.choice(WORDS[1:6]) for _ in range(n)], pa.string()),
        "s_nulls": pa.array([None if rng.random() < 0.6 else _text(rng, 1, 10) for _ in range(n)], pa.string()),
        "s_long": pa.array([_text(rng, 1000, 2000) if i % 125 == 7 else _text(rng, 0, 3) for i in range(n)], pa.string()),
        "bin": pa.array([_text(rng, 0, 20).encode() for _ in range(n)], pa.binary()),
        "fixed8": pa.array([_maybe_null(rng, _text(rng, 8, 8).encode()) for _ in range(n)], pa.binary(8)),
    }
    return pa.table(cols)


def ds_numbers(n=500):
    """Numbers for DELTA_BINARY_PACKED / BYTE_STREAM_SPLIT: monotone, random, small, floats."""
    rng = _rng("numbers")
    mono = []
    v = -(1 << 40)
    for _ in range(n):
        v += rng.randint(0, 1 << rng.randint(0, 20))
        mono.append(v)
    cols = {
        "id": pa.array(range(n), pa.int64()),
        "i32_mono": pa.array([i * 3 - 500 for i in range(n)], pa.int32()),
        "i64_mono": pa.array(mono, pa.int64()),
        "i64_rand": pa.array(_int_column(rng, n, 64, True, 0.05), pa.int64()),
        "i32_rand": pa.array(_int_column(rng, n, 32, True, 0.05), pa.int32()),
        "i32_small": pa.array([_maybe_null(rng, rng.randint(0, 7), 0.05) for _ in range(n)], pa.int32()),
        "u64_rand": pa.array(_int_column(rng, n, 64, False, 0.05), pa.uint64()),
        "f32": pa.array([_maybe_null(rng, rng.uniform(-1e3, 1e3), 0.05) for _ in range(n)], pa.float32()),
        "f64": pa.array([_maybe_null(rng, rng.gauss(0, 1e10), 0.05) for _ in range(n)], pa.float64()),
        "dec18": pa.array([_maybe_null(rng, _decimal(rng, 18, 3), 0.05) for _ in range(n)], pa.decimal128(18, 3)),
        "d": pa.array([_maybe_null(rng, rng.randint(-25000, 100000), 0.05) for _ in range(n)], pa.int32()).cast(pa.date32()),
        "ts_us": pa.array([1600000000 * 10**6 + i * 1000003 for i in range(n)], pa.int64()).cast(pa.timestamp("us")),
    }
    return pa.table(cols)


def ds_decimals(n=120):
    """Decimals of many precisions, to exercise INT32/INT64/FLBA/BYTE_ARRAY physical types."""
    rng = _rng("decimals")
    cols = {"id": pa.array(range(n), pa.int64())}
    for p, s in [(1, 0), (4, 2), (9, 0), (9, 9), (10, 3), (18, 0), (18, 18), (19, 5), (25, 10), (38, 0), (38, 38)]:
        vals = [decimal.Decimal(10 ** p - 1).scaleb(-s), decimal.Decimal(-(10 ** p - 1)).scaleb(-s), decimal.Decimal(0)]
        vals += [_maybe_null(rng, _decimal(rng, p, s)) for _ in range(n - 3)]
        cols[f"dec_{p}_{s}"] = pa.array(vals, pa.decimal128(p, s))
    return pa.table(cols)


def ds_wide(n=5, ncols=200):
    """Very wide schema: many columns of alternating types, few rows."""
    rng = _rng("wide")
    cols = {"id": pa.array(range(n), pa.int64())}
    for c in range(ncols):
        kind = c % 4
        if kind == 0:
            cols[f"c{c}"] = pa.array([_maybe_null(rng, rng.randint(-10**6, 10**6)) for _ in range(n)], pa.int32())
        elif kind == 1:
            cols[f"c{c}"] = pa.array([_maybe_null(rng, _text(rng, 0, 8)) for _ in range(n)], pa.string())
        elif kind == 2:
            cols[f"c{c}"] = pa.array([_maybe_null(rng, rng.uniform(-1, 1)) for _ in range(n)], pa.float64())
        else:
            cols[f"c{c}"] = pa.array([_maybe_null(rng, rng.random() < 0.5) for _ in range(n)], pa.bool_())
    return pa.table(cols)


def ds_all_nulls(n=100):
    """Every column is entirely null (all-null pages, dictionary with zero entries)."""
    flat = ds_flat(20)
    cols = {"id": pa.array(range(n), pa.int64())}
    for f in flat.schema:
        if f.name != "id":
            cols[f.name] = pa.nulls(n, f.type)
    cols["l_i32"] = pa.nulls(n, pa.list_(pa.int32()))
    cols["st"] = pa.nulls(n, pa.struct([("a", pa.int32()), ("b", pa.string())]))
    return pa.table(cols)


def ds_empty():
    """Zero rows, flat and nested schema."""
    flat = ds_flat(20)
    nested = ds_nested(20)
    fields = list(flat.schema) + [f for f in nested.schema if f.name != "id"]
    return pa.schema(fields).empty_table()


def ds_names():
    """Column names that need quoting or collide with ClickHouse subcolumn syntax."""
    return pa.table({
        "id": pa.array(range(10), pa.int64()),
        "Mixed Case Name": pa.array(range(10), pa.int32()),
        "with-dash and space": pa.array([str(i) for i in range(10)]),
        "ünïcødé_列": pa.array([float(i) for i in range(10)]),
        "dots.in.name": pa.array([i % 2 == 0 for i in range(10)]),
        "`backtick`": pa.array(range(10), pa.int16()),
        "select": pa.array([str(i) for i in range(10)]),
    })


def ds_logical(n=50):
    """Logical types that only newer writers support: float16, UUID, JSON, time, large types, dictionary, decimal256."""
    rng = _rng("logical")
    cols = {
        "id": pa.array(range(n), pa.int64()),
        "f16": pa.array([_maybe_null(rng, rng.choice([0.0, -0.0, 1.0, 0.5, -2.25, 65504.0, 6.1e-05, 1024.0])) for _ in range(n)], pa.float32()).cast(pa.float16()),
        "uuid": pa.ExtensionArray.from_storage(pa.uuid(), pa.array([_maybe_null(rng, uuidlib.UUID(int=rng.getrandbits(128)).bytes) for _ in range(n)], pa.binary(16))),
        "t32_ms": pa.array([_maybe_null(rng, rng.randint(0, 86400 * 1000 - 1)) for _ in range(n)], pa.int32()).cast(pa.time32("ms")),
        "t64_us": pa.array([_maybe_null(rng, rng.randint(0, 86400 * 10**6 - 1)) for _ in range(n)], pa.int64()).cast(pa.time64("us")),
        "large_s": pa.array([_maybe_null(rng, rng.choice(WORDS)) for _ in range(n)], pa.large_string()),
        "large_bin": pa.array([_maybe_null(rng, _text(rng).encode()) for _ in range(n)], pa.large_binary()),
        "dict_s": pa.array([_maybe_null(rng, rng.choice(WORDS[1:5])) for _ in range(n)], pa.string()).dictionary_encode(),
        "dec76": pa.array([_maybe_null(rng, _decimal(rng, 76, 20)) for _ in range(n)], pa.decimal256(76, 20)),
        "large_l": pa.array([_maybe_null(rng, [rng.randint(0, 9) for _ in range(rng.randint(0, 3))]) for _ in range(n)], pa.large_list(pa.int16())),
        "fsl": pa.array([_maybe_null(rng, [rng.randint(0, 9) for _ in range(3)]) for _ in range(n)], pa.list_(pa.int8(), 3)),
        "date64": pa.array([_maybe_null(rng, rng.randint(-10000, 50000) * 86400000) for _ in range(n)], pa.int64()).cast(pa.date64()),
    }
    return pa.table(cols)


DATASETS = {
    "flat": ds_flat,
    "nested": ds_nested,
    "strings": ds_strings,
    "numbers": ds_numbers,
    "decimals": ds_decimals,
    "wide": ds_wide,
    "all_nulls": ds_all_nulls,
    "empty": ds_empty,
    "names": ds_names,
    "logical": ds_logical,
}


def write_ipc(table, path):
    with pa.OSFile(str(path), "wb") as f:
        with ipc.new_file(f, table.schema) as w:
            w.write_table(table)


def read_ipc(path):
    with pa.memory_map(str(path)) as f:
        return ipc.open_file(f).read_all()


# ---------------------------------------------------------------------------
# Normalization of a readback table into JSON values matching ClickHouse semantics
# ---------------------------------------------------------------------------
#
# ClickHouse reads a Parquet file into its own types, then the expected JSON Lines are
# parsed into exactly those types with `JSONEachRow`. The values below are therefore what
# the original writer stored, in a textual form that `JSONEachRow` parses unambiguously:
#  - a null list or map becomes empty (`Array` and `Map` are not `Nullable`);
#  - a null struct becomes a struct of nulls (`Tuple` is not `Nullable`);
#  - timestamps are formatted in UTC with the precision of their unit;
#  - decimals are strings, NaN and infinities are strings.


# The corpus datasets only have UTF-8 binary values (JSON cannot hold other bytes); the round trip
# report compares in memory and sets this to "surrogateescape" to tolerate other values.
BINARY_DECODE_ERRORS = "strict"


def _fmt_ts(value, unit):
    per_sec = {"s": 1, "ms": 1000, "us": 10**6, "ns": 10**9}[unit]
    secs, frac = divmod(value, per_sec)
    dt = EPOCH + datetime.timedelta(seconds=secs)
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    if per_sec == 1:
        return base
    digits = len(str(per_sec)) - 1
    return f"{base}.{frac:0{digits}d}"


def _fmt_float(v):
    if v is None:
        return None
    if math.isnan(v):
        return "nan"
    if math.isinf(v):
        return "inf" if v > 0 else "-inf"
    return v


def _null_of(t):
    """Value ClickHouse produces for a null of Arrow type `t` in a non-Nullable position."""
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return []
    if pa.types.is_map(t):
        return {}
    if pa.types.is_struct(t):
        return {t.field(i).name: _null_of(t.field(i).type) for i in range(t.num_fields)}
    return None


def norm_array(arr, uuid=False):
    """Normalize a (non-chunked) Arrow array into a list of JSON-compatible values.

    `uuid`: the column is a UUID stored as 16-byte binary without the Arrow UUID extension type.
    """
    t = arr.type
    if uuid:
        if isinstance(arr, pa.ChunkedArray):
            arr = arr.combine_chunks()
        if isinstance(arr.type, pa.BaseExtensionType):
            arr = arr.storage
        return [None if v is None else str(uuidlib.UUID(bytes=v)) for v in arr.to_pylist()]
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks() if arr.num_chunks else pa.array([], t)
        t = arr.type
    if pa.types.is_dictionary(t):
        return norm_array(arr.dictionary_decode())
    if pa.types.is_string_view(t):
        return norm_array(arr.cast(pa.large_string()))
    if pa.types.is_binary_view(t):
        return norm_array(arr.cast(pa.large_binary()))
    if pa.types.is_list_view(t) or pa.types.is_large_list_view(t):
        return norm_array(arr.cast(pa.large_list(t.value_type)))
    if isinstance(t, pa.BaseExtensionType):
        if t.extension_name == "arrow.uuid":
            return [None if v is None else str(uuidlib.UUID(bytes=v)) for v in arr.storage.to_pylist()]
        return norm_array(arr.storage)
    valid = arr.is_valid().to_pylist() if len(arr) else []
    if pa.types.is_struct(t):
        children = [norm_array(c) for c in arr.flatten()]
        names = [t.field(i).name for i in range(t.num_fields)]
        out = []
        for i in range(len(arr)):
            if not valid[i]:
                out.append(_null_of(t))
            else:
                out.append({names[j]: children[j][i] for j in range(len(names))})
        return out
    if pa.types.is_map(t):
        offsets = arr.offsets.to_pylist()
        keys = norm_array(arr.keys)
        items = norm_array(arr.items)
        out = []
        for i in range(len(arr)):
            if not valid[i]:
                out.append({})
            else:
                out.append({(k if isinstance(k, str) else json.dumps(k)): v for k, v in zip(keys[offsets[i]:offsets[i + 1]], items[offsets[i]:offsets[i + 1]])})
        return out
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        offsets = arr.offsets.to_pylist()
        child = norm_array(arr.values)
        return [child[offsets[i]:offsets[i + 1]] if valid[i] else [] for i in range(len(arr))]
    if pa.types.is_fixed_size_list(t):
        size = t.list_size
        child = norm_array(arr.values)
        base = arr.offset * size
        return [child[base + i * size: base + (i + 1) * size] if valid[i] else [] for i in range(len(arr))]
    if pa.types.is_timestamp(t):
        ints = arr.cast(pa.int64()).to_pylist()
        return [None if v is None else _fmt_ts(v, t.unit) for v in ints]
    if pa.types.is_date32(t):
        ints = arr.cast(pa.int32()).to_pylist()
        return [None if v is None else (EPOCH_DATE + datetime.timedelta(days=v)).isoformat() for v in ints]
    if pa.types.is_date64(t):
        ints = arr.cast(pa.int64()).to_pylist()
        return [None if v is None else (EPOCH_DATE + datetime.timedelta(days=v // 86400000)).isoformat() for v in ints]
    if pa.types.is_time32(t) or pa.types.is_time64(t):
        ints = arr.cast(pa.int64() if pa.types.is_time64(t) else pa.int32()).to_pylist()
        return ints
    if pa.types.is_decimal(t):
        return [None if v is None else format(v, "f") for v in arr.to_pylist()]
    if pa.types.is_floating(t):
        if pa.types.is_float16(t):
            arr = arr.cast(pa.float32())
        return [_fmt_float(v) for v in arr.to_pylist()]
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return [None if v is None else v.decode("utf-8", errors=BINARY_DECODE_ERRORS) for v in arr.to_pylist()]
    if pa.types.is_null(t):
        return [None] * len(arr)
    if pa.types.is_duration(t):
        return arr.cast(pa.int64()).to_pylist()
    return arr.to_pylist()


def normalize_table(table, uuid_columns=()):
    columns = [norm_array(table.column(i), uuid=table.column_names[i] in uuid_columns) for i in range(table.num_columns)]
    names = table.column_names
    return [{names[j]: columns[j][i] for j in range(len(names))} for i in range(table.num_rows)]


def rows_to_jsonl_zst(rows):
    buf = io.StringIO()
    for r in rows:
        buf.write(json.dumps(r, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
        buf.write("\n")
    raw = buf.getvalue().encode()
    return zstandard.ZstdCompressor(level=19).compress(raw), raw


def jsonl_digest(raw):
    return hashlib.sha256(raw).hexdigest()[:16]
