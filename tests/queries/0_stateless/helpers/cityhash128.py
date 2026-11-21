"""CityHash128 v1.0.2 in pure Python, a port of `contrib/cityhash102/src/city.cc`.

The checksum of a compressed block in ClickHouse is this hash of the block, header included,
so a test that crafts compressed blocks by hand needs it to get them past the checksum check.
"""

import struct

M = (1 << 64) - 1
k0, k1, k2, k3 = (
    0xC3A5C85C97CB3127,
    0xB492B66FBE98F273,
    0x9AE16A3B2F90404F,
    0xC949D7C7509E6557,
)


def f64(s, p):
    return struct.unpack_from("<Q", s, p)[0]


def f32(s, p):
    return struct.unpack_from("<I", s, p)[0]


def rot(v, sh):
    return v if sh == 0 else ((v >> sh) | (v << (64 - sh))) & M


def shift_mix(v):
    return v ^ (v >> 47)


def hash_len16(u, v):
    kmul = 0x9DDFEA08EB382D69
    a = ((u ^ v) * kmul) & M
    a ^= a >> 47
    b = ((v ^ a) * kmul) & M
    b ^= b >> 47
    return (b * kmul) & M


def hash_len0to16(s, p, n):
    if n > 8:
        a = f64(s, p)
        b = f64(s, p + n - 8)
        sh = n & 63
        return (
            hash_len16(a, ((((b + n) & M) >> sh) | (((b + n) & M) << (64 - sh))) & M)
            ^ b
        )
    if n >= 4:
        a = f32(s, p)
        return hash_len16((n + (a << 3)) & M, f32(s, p + n - 4))
    if n > 0:
        a, b, c = s[p], s[p + (n >> 1)], s[p + n - 1]
        y = (a + (b << 8)) & 0xFFFFFFFF
        z = (n + (c << 2)) & 0xFFFFFFFF
        return (shift_mix(((y * k2) & M) ^ ((z * k3) & M)) * k2) & M
    return k2


def weak32(w, x, y, z, a, b):
    a = (a + w) & M
    b = rot((b + a + z) & M, 21)
    c = a
    a = (a + x) & M
    a = (a + y) & M
    b = (b + rot(a, 44)) & M
    return (a + z) & M, (b + c) & M


def weak32s(s, p, a, b):
    return weak32(f64(s, p), f64(s, p + 8), f64(s, p + 16), f64(s, p + 24), a, b)


def city_murmur(s, p, n, seed):
    a, b = seed
    c = d = 0
    l = n - 16
    if l <= 0:
        a = (shift_mix((a * k1) & M) * k1) & M
        c = ((b * k1) + hash_len0to16(s, p, n)) & M
        d = shift_mix((a + (f64(s, p) if n >= 8 else c)) & M)
    else:
        c = hash_len16((f64(s, p + n - 8) + k1) & M, a)
        d = hash_len16((b + n) & M, (c + f64(s, p + n - 16)) & M)
        a = (a + d) & M
        while True:
            a ^= (shift_mix((f64(s, p) * k1) & M) * k1) & M
            a = (a * k1) & M
            b ^= a
            c ^= (shift_mix((f64(s, p + 8) * k1) & M) * k1) & M
            c = (c * k1) & M
            d ^= c
            p += 16
            l -= 16
            if l <= 0:
                break
    a = hash_len16(a, c)
    b = hash_len16(d, b)
    return a ^ b, hash_len16(b, a)


def city128_seed(s, p, n, seed):
    if n < 128:
        return city_murmur(s, p, n, seed)
    x, y = seed
    z = (n * k1) & M
    v0 = (rot(y ^ k1, 49) * k1 + f64(s, p)) & M
    v1 = (rot(v0, 42) * k1 + f64(s, p + 8)) & M
    w0 = (rot((y + z) & M, 35) * k1 + x) & M
    w1 = (rot((x + f64(s, p + 88)) & M, 53) * k1) & M
    while True:
        for _ in range(2):
            x = (rot((x + y + v0 + f64(s, p + 16)) & M, 37) * k1) & M
            y = (rot((y + v1 + f64(s, p + 48)) & M, 42) * k1) & M
            x ^= w1
            y ^= v0
            z = rot(z ^ w0, 33)
            v0, v1 = weak32s(s, p, (v1 * k1) & M, (x + w0) & M)
            w0, w1 = weak32s(s, p + 32, (z + w1) & M, y)
            z, x = x, z
            p += 64
        n -= 128
        if n < 128:
            break
    y = (y + rot(w0, 37) * k0 + z) & M
    x = (x + rot((v0 + z) & M, 49) * k0) & M
    tail_done = 0
    while tail_done < n:
        tail_done += 32
        y = (rot((y - x) & M, 42) * k0 + v1) & M
        w0 = (w0 + f64(s, p + n - tail_done + 16)) & M
        x = (rot(x, 49) * k0 + w0) & M
        w0 = (w0 + v0) & M
        v0, v1 = weak32s(s, p + n - tail_done, v0, v1)
    x = hash_len16(x, v0)
    y = hash_len16(y, w0)
    return (hash_len16((x + v1) & M, w1) + y) & M, hash_len16(
        (x + w1) & M, (y + v1) & M
    )


def city128(s):
    n = len(s)
    if n >= 16:
        return city128_seed(s, 16, n - 16, (f64(s, 0) ^ k3, f64(s, 8)))
    if n >= 8:
        return city128_seed(b"", 0, 0, (f64(s, 0) ^ ((n * k0) & M), f64(s, n - 8) ^ k1))
    return city128_seed(s, 0, n, (k0, k1))


def checksum(block):
    """The 16 bytes that precede a compressed block on the wire and on disk."""
    low, high = city128(block)
    return struct.pack("<QQ", low, high)
