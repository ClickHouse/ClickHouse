#!/usr/bin/env bash
# Tags: no-fasttest
# ^ the AWS Signature Version 4 authentication needs SSL

# The file-like HTTP API (`/database/table.format`) authenticated with AWS Signature Version 4, as by S3 clients.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="u_sigv4_${CLICKHOUSE_DATABASE}"
HASHED_USER_NAME="u_sigv4_hashed_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_NAME}, ${HASHED_USER_NAME};
    CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'secret';
    CREATE USER ${HASHED_USER_NAME} IDENTIFIED WITH sha256_password BY 'secret';
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1, 'Hello'), (2, 'World');
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.t TO ${USER_NAME}, ${HASHED_USER_NAME};
"

python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" "${CLICKHOUSE_DATABASE}" "${USER_NAME}" "${HASHED_USER_NAME}" <<'EOF'
import datetime
import hashlib
import hmac
import http.client
import re
import sys
import urllib.parse

host, port, database, user, hashed_user = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
host_header = f"{host}:{port}"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def quote(s, safe=""):
    return urllib.parse.quote(s, safe=safe + "-_.~")


def hmac_sha256(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def signature(secret, date, string_to_sign, region="us-east-1"):
    key = hmac_sha256(("AWS4" + secret).encode(), date)
    for part in (region, "s3", "aws4_request"):
        key = hmac_sha256(key, part)
    return hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()


def canonical_request(method, path, params, headers, signed_headers, payload_hash):
    query = "&".join(f"{quote(k)}={quote(v)}" for k, v in sorted(params))
    canonical_headers = "".join(f"{h}:{headers[h].strip()}\n" for h in signed_headers)
    return "\n".join([method, quote(path, "/"), query, canonical_headers, ";".join(signed_headers), payload_hash])


def string_to_sign(amz_date, creq):
    scope = f"{amz_date[:8]}/us-east-1/s3/aws4_request"
    return "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(creq.encode()).hexdigest()])


def send(method, path, params=(), headers=None, body=b""):
    conn = http.client.HTTPConnection(host, port)
    url = quote(path, "/")
    if params:
        url += "?" + "&".join(f"{quote(k)}={quote(v)}" for k, v in params)
    all_headers = {"Host": host_header}
    all_headers.update(headers or {})
    if method in ("PUT", "POST") or body:
        all_headers.setdefault("Content-Length", str(len(body)))
    conn.putrequest(method, url, skip_host=True, skip_accept_encoding=True)
    for k, v in all_headers.items():
        conn.putheader(k, v)
    conn.endheaders()
    if body:
        conn.send(body)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return response, data


def signed_request(method, path, params=(), headers=None, body=b"", payload_hash=None, access_key=user, secret="secret",
                   sign_headers=None, amz_date=None):
    amz_date = amz_date or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload_hash = payload_hash or hashlib.sha256(body).hexdigest()
    headers = dict(headers or {})
    all_headers = {"host": host_header, "x-amz-date": amz_date, "x-amz-content-sha256": payload_hash}
    for k, v in headers.items():
        if sign_headers is None or k.lower() in sign_headers:
            all_headers[k.lower()] = v
    signed_headers = sorted(all_headers)
    creq = canonical_request(method, path, params, all_headers, signed_headers, payload_hash)
    sts = string_to_sign(amz_date, creq)
    sig = signature(secret, amz_date[:8], sts)
    request_headers = {k: v for k, v in all_headers.items() if k != "host"}
    request_headers.update({k: v for k, v in headers.items() if k.lower() not in all_headers})
    request_headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{amz_date[:8]}/us-east-1/s3/aws4_request, "
        f"SignedHeaders={';'.join(signed_headers)}, Signature={sig}")
    return send(method, path, params, request_headers, body)


def presigned_params(method, path, params=(), expires=60, amz_date=None, secret="secret"):
    amz_date = amz_date or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    all_params = list(params) + [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", f"{user}/{amz_date[:8]}/us-east-1/s3/aws4_request"),
        ("X-Amz-Date", amz_date),
        ("X-Amz-Expires", str(expires)),
        ("X-Amz-SignedHeaders", "host"),
    ]
    creq = canonical_request(method, path, all_params, {"host": host_header}, ["host"], "UNSIGNED-PAYLOAD")
    sig = signature(secret, amz_date[:8], string_to_sign(amz_date, creq))
    return all_params + [("X-Amz-Signature", sig)]


def show(name, result):
    response, data = result
    text = data.decode(errors="replace").strip()
    if response.status != 200:
        # Keep only the error code, the message contains variable parts.
        code = re.search(r"Code: (\d+)", text)
        text = code.group(0) if code else text[:20]
    print(f"{name}: {response.status} {text}")


table_path = f"/{database}/t.TSV"
csv_path = f"/{database}/t.CSV"

show("GET", signed_request("GET", table_path))
show("GET with a query parameter", signed_request("GET", table_path, params=[("x-id", "GetObject"), ("select", "s")]))

response, data = signed_request("HEAD", table_path)
print("HEAD:", response.status, response.getheader("Content-Length"), len(data), response.getheader("Last-Modified") is not None)

show("GET presigned", send("GET", table_path, presigned_params("GET", table_path)))
show("GET presigned with a query", send("GET", "/", presigned_params("GET", "/", [("query", "SELECT 42")])))

# Authentication failures.
show("wrong secret", signed_request("GET", table_path, secret="wrong"))
show("unknown user", signed_request("GET", table_path, access_key="nonexistent_sigv4_user"))
show("password stored as a hash", signed_request("GET", table_path, access_key=hashed_user))
show("presigned with wrong secret", send("GET", table_path, presigned_params("GET", table_path, secret="wrong")))
show("presigned and expired", send("GET", table_path, presigned_params("GET", table_path, expires=60, amz_date="20200101T000000Z")))
show("request too old", signed_request("GET", table_path, amz_date="20200101T000000Z"))
show("unsigned X-ClickHouse header", signed_request("GET", table_path, headers={"X-ClickHouse-Format": "JSON"}, sign_headers=set()))
show("signed X-ClickHouse header", signed_request("GET", table_path, headers={"X-ClickHouse-Format": "CSV"}))
show("tampered query", send("GET", "/", [(k, "SELECT 43" if k == "query" else v) for k, v in presigned_params("GET", "/", [("query", "SELECT 42")])]))
show("presigned with a body", send("POST", "/", presigned_params("POST", "/"), body=b"SELECT 1"))
show("unsigned payload with a body", signed_request("POST", "/", body=b"SELECT 1", payload_hash="UNSIGNED-PAYLOAD"))
show("signed payload with a body", signed_request("POST", "/", body=b"SELECT 1"))

# Range requests are not supported, except for a range from the beginning, which is ignored.
show("range from the beginning", signed_request("GET", table_path, headers={"Range": "bytes=0-15"}, params=[("order", "x")]))
show("range", signed_request("GET", table_path, headers={"Range": "bytes=3-5"}))

# Uploads.
show("PUT", signed_request("PUT", csv_path, body=b"3,\"PUT\"\n"))
show("PUT with unsigned payload", signed_request("PUT", csv_path, body=b"4,\"unsigned\"\n", payload_hash="UNSIGNED-PAYLOAD"))
show("PUT presigned", send("PUT", csv_path, presigned_params("PUT", csv_path), body=b"5,\"presigned\"\n"))
chunk = b"6,\"chunked\"\n"
chunked_body = b"%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n" % (len(chunk), chunk)
show("PUT aws-chunked", signed_request("PUT", csv_path, body=chunked_body, payload_hash="STREAMING-UNSIGNED-PAYLOAD-TRAILER",
     headers={"Content-Encoding": "aws-chunked", "X-Amz-Decoded-Content-Length": str(len(chunk))}))
show("PUT with wrong payload hash", signed_request("PUT", csv_path, body=b"7,\"wrong\"\n", payload_hash=EMPTY_SHA256))
show("PUT with signed streaming payload", signed_request("PUT", csv_path, body=b"8,\"x\"\n", payload_hash="STREAMING-AWS4-HMAC-SHA256-PAYLOAD"))
show("GET after PUT", signed_request("GET", table_path, params=[("order", "x")]))
EOF

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER_NAME}, ${HASHED_USER_NAME}"
