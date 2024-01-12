#!/usr/bin/python3
import sys, requests, json, re, base64, os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

#
# Script to upload/download to/from pastila.nl from command line.
# Put it somewhere in PATH and use like this:
#
# % echo henlo | pastila.py
# https://pastila.nl/?cafebabe/c8570701492af2ac7269064a661686e3#oj6jpBCRzIOSGTgRFgMquA==
#
# % pastila.py 'https://pastila.nl/?cafebabe/c8570701492af2ac7269064a661686e3#oj6jpBCRzIOSGTgRFgMquA=='
# henlo
#


def sipHash128(m: bytes):
    mask = (1 << 64) - 1

    def rotl(v, offset, bits):
        v[offset] = ((v[offset] << bits) & mask) | ((v[offset] & mask) >> (64 - bits))

    def compress(v):
        v[0] += v[1]
        v[2] += v[3]
        rotl(v, 1, 13)
        rotl(v, 3, 16)
        v[1] ^= v[0]
        v[3] ^= v[2]
        rotl(v, 0, 32)
        v[2] += v[1]
        v[0] += v[3]
        rotl(v, 1, 17)
        rotl(v, 3, 21)
        v[1] ^= v[2]
        v[3] ^= v[0]
        rotl(v, 2, 32)

    v = [0x736F6D6570736575, 0x646F72616E646F6D, 0x6C7967656E657261, 0x7465646279746573]
    offset = 0
    while offset < len(m) - 7:
        word = int.from_bytes(m[offset : offset + 8], "little")
        v[3] ^= word
        compress(v)
        compress(v)
        v[0] ^= word
        offset += 8

    buf = bytearray(8)
    buf[: len(m) - offset] = m[offset:]
    buf[7] = len(m) & 0xFF

    word = int.from_bytes(buf, "little")
    v[3] ^= word
    compress(v)
    compress(v)
    v[0] ^= word
    v[2] ^= 0xFF
    compress(v)
    compress(v)
    compress(v)
    compress(v)

    hash_val = ((v[0] ^ v[1]) & mask) + (((v[2] ^ v[3]) & mask) << 64)
    s = "{:032x}".format(hash_val)
    return "".join(s[i : i + 2] for i in range(30, -2, -2))


def error(s):
    sys.stderr.write(f"error: {s}\n")
    sys.exit(1)


# This is too slow, and doesn't seem important.
# def getFingerprint(text):
#    words = re.findall(r'\b\w{4,}\b', text.decode()) # doesn't exactly match the JS code, but it doesn't have to
#    triplets = [''.join(words[i:i+3]) for i in range(len(words) - 2)]
#    uniq = set(triplets)
#    hashes = [sipHash128(s.encode())[:8] for s in uniq]
#    hashes.append('ffffffff')
#    return min(hashes)


def load(url):
    r = re.match(
        r"^(?:(?:(?:(?:(?:https?:)?//)?pastila\.nl)?/)?\?)?([a-f0-9]+)/([a-f0-9]+)(?:#(.+))?$",
        url,
    )
    if r is None:
        error("bad url")
    fingerprint, hash_hex, key = r.groups()

    response = requests.post(
        "https://play.clickhouse.com/?user=paste",
        data=f"SELECT content, is_encrypted FROM data WHERE fingerprint = reinterpretAsUInt32(unhex('{fingerprint}')) AND hash = reinterpretAsUInt128(unhex('{hash_hex}')) ORDER BY time LIMIT 1 FORMAT JSON",
    )
    if not response.ok:
        error(f"{response} {response.content}")

    j = json.loads(response.content)
    if j["rows"] != 1:
        error("paste not found")
    # if 'statistics' in j: sys.stderr.write(f"{j['statistics']}")
    content, is_encrypted = j["data"][0]["content"], j["data"][0]["is_encrypted"]

    if is_encrypted:
        if key is None:
            error("paste is encrypted, but the url contains no key (part after '#')")
        key = base64.b64decode(key)
        content = base64.b64decode(content)
        cipher = Cipher(
            algorithms.AES(key), modes.CTR(b"\x00" * 16), backend=default_backend()
        )
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(content) + decryptor.finalize()
        content = decrypted

    return content


def save(data, encrypt):
    key = os.urandom(16)
    url_suffix = ""
    if encrypt:
        cipher = Cipher(
            algorithms.AES(key), modes.CTR(b"\x00" * 16), backend=default_backend()
        )
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(data) + encryptor.finalize()
        data = base64.b64encode(encrypted)
        url_suffix = "#" + base64.b64encode(key).decode()

    h = sipHash128(data)
    fingerprint = "cafebabe"  # getFingerprint(data)

    payload = json.dumps(
        {
            "fingerprint_hex": fingerprint,
            "hash_hex": h,
            "content": data.decode(),
            "is_encrypted": encrypt,
        }
    )
    response = requests.post(
        "https://play.clickhouse.com/?user=paste",
        data=f"INSERT INTO data (fingerprint_hex, hash_hex, content, is_encrypted) FORMAT JSONEachRow {payload}",
    )
    if not response.ok:
        error(f"{response} {response.content}")
    print(f"https://pastila.nl/?{fingerprint}/{h}{url_suffix}")


if len(sys.argv) == 1:
    data = sys.stdin.buffer.read()
    save(data, True)
elif len(sys.argv) == 2 and sys.argv[1] == "plain":
    data = sys.stdin.buffer.read()
    save(data, False)
elif len(sys.argv) == 2:
    data = load(sys.argv[1])
    sys.stdout.buffer.write(data)
else:
    print("usage: pastila.py [url]")
    sys.exit(1)
