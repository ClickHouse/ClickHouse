#!/usr/bin/env python3
"""A minimal native-protocol peer for `ssh_key` authentication, over plain TCP.

It can play a client that announces any protocol revision and signs either message layout,
or an old server that only knows the legacy layout. Signatures are made and checked with
`ssh-keygen -Y`, in the same SSHSIG format (namespace `clickhouse`, SHA-256) as the server.

Client:
    ssh_key_auth_peer.py client <host> <port> <user> <private key file> <announced revision> <layout> <signed revision>

    Sends `Hello` with <announced revision>, requests a challenge, and signs the <layout> message
    (`legacy` or `bound`, the latter with the `none` binding of a plaintext connection)
    with <signed revision> as the version inside it. Prints the challenge packet it has received,
    then `authenticated` or the name of the exception the server has sent.

Old server:
    ssh_key_auth_peer.py old-server <port file>

    Accepts one connection, sends the legacy `SSHChallenge`, and prints whether the signature
    is valid for the legacy message, built from the client's `Hello`. Then fails the authentication.
"""

import os
import secrets
import socket
import subprocess
import sys
import tempfile

SSH_KEY_AUTHENTICATION_MARKER = " SSH KEY AUTHENTICATION "
MESSAGE_TAG = "ClickHouse native ssh_key auth v1"

CLIENT_HELLO = 0
CLIENT_SSH_CHALLENGE_REQUEST = 11
CLIENT_SSH_CHALLENGE_RESPONSE = 12
SERVER_HELLO = 0
SERVER_EXCEPTION = 2
SERVER_SSH_CHALLENGE = 18
SERVER_SSH_CHALLENGE_WITH_BINDING = 19
PACKET_NAMES = {SERVER_SSH_CHALLENGE: "SSHChallenge", SERVER_SSH_CHALLENGE_WITH_BINDING: "SSHChallengeWithBinding"}


def var_uint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def string(value):
    if isinstance(value, str):
        value = value.encode()
    return var_uint(len(value)) + value


class Reader:
    def __init__(self, sock):
        self.sock = sock

    def exactly(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise EOFError("connection closed")
            data += chunk
        return data

    def var_uint(self):
        result = 0
        shift = 0
        while True:
            byte = self.exactly(1)[0]
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7

    def string(self):
        return self.exactly(self.var_uint())


def error_code_name(code):
    return {516: "AUTHENTICATION_FAILED"}.get(code, str(code))


def legacy_message(revision, database, user, challenge):
    return str(revision).encode() + database + user + challenge


def bound_message(revision, database, user, challenge):
    fields = [MESSAGE_TAG.encode(), str(revision).encode(), database, user, challenge, b"none", b""]
    return b"".join(string(field) for field in fields)


def sign(key_file, message):
    result = subprocess.run(
        ["ssh-keygen", "-Y", "sign", "-n", "clickhouse", "-O", "hashalg=sha256", "-f", key_file],
        input=message,
        capture_output=True,
        check=True,
    )
    return result.stdout


def signature_is_valid(signature, message):
    with tempfile.NamedTemporaryFile(suffix=".sig") as signature_file:
        signature_file.write(signature)
        signature_file.flush()
        result = subprocess.run(
            ["ssh-keygen", "-Y", "check-novalidate", "-n", "clickhouse", "-s", signature_file.name],
            input=message,
            capture_output=True,
        )
    return result.returncode == 0


def run_client(host, port, user, key_file, announced_revision, layout, signed_revision):
    sock = socket.create_connection((host, port), timeout=60)
    reader = Reader(sock)
    database = b""
    sock.sendall(
        var_uint(CLIENT_HELLO)
        + string("ssh_key_auth_peer")
        + var_uint(26)
        + var_uint(1)
        + var_uint(announced_revision)
        + string(database)
        + string(SSH_KEY_AUTHENTICATION_MARKER + user)
        + string("")
        + var_uint(CLIENT_SSH_CHALLENGE_REQUEST)
    )

    packet = reader.var_uint()
    if packet == SERVER_EXCEPTION:
        print("exception", error_code_name(int.from_bytes(reader.exactly(4), "little", signed=True)))
        return
    print("received", PACKET_NAMES.get(packet, packet))
    challenge = reader.string()

    build = legacy_message if layout == "legacy" else bound_message
    signature = sign(key_file, build(signed_revision, database, user.encode(), challenge))
    sock.sendall(var_uint(CLIENT_SSH_CHALLENGE_RESPONSE) + string(signature))

    packet = reader.var_uint()
    if packet == SERVER_HELLO:
        print("authenticated")
    elif packet == SERVER_EXCEPTION:
        print("exception", error_code_name(int.from_bytes(reader.exactly(4), "little", signed=True)))
    else:
        print("unexpected packet", packet)


def run_old_server(port_file):
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(60)
    with open(port_file + ".tmp", "w") as f:
        f.write(str(listener.getsockname()[1]))
    os.rename(port_file + ".tmp", port_file)

    sock, _ = listener.accept()
    listener.close()
    sock.settimeout(60)
    reader = Reader(sock)

    assert reader.var_uint() == CLIENT_HELLO
    reader.string()  # client name
    reader.var_uint()  # major
    reader.var_uint()  # minor
    revision = reader.var_uint()
    database = reader.string()
    user = reader.string()
    reader.string()  # password
    assert user.startswith(SSH_KEY_AUTHENTICATION_MARKER.encode())
    user = user[len(SSH_KEY_AUTHENTICATION_MARKER) :]
    assert reader.var_uint() == CLIENT_SSH_CHALLENGE_REQUEST

    challenge = secrets.token_hex(32).encode()
    sock.sendall(var_uint(SERVER_SSH_CHALLENGE) + string(challenge))
    assert reader.var_uint() == CLIENT_SSH_CHALLENGE_RESPONSE
    signature = reader.string()

    if signature_is_valid(signature, legacy_message(revision, database, user, challenge)):
        print("the signature is valid for the legacy message")
    else:
        print("the signature is NOT valid for the legacy message")

    # Let the client finish with an error instead of a half-done handshake.
    code_authentication_failed = 516
    sock.sendall(
        var_uint(SERVER_EXCEPTION)
        + code_authentication_failed.to_bytes(4, "little")
        + string("DB::Exception")
        + string("old server test peer: done")
        + string("")
        + b"\x00"
    )
    sock.close()


if __name__ == "__main__":
    if sys.argv[1] == "client":
        host, port, user, key_file, announced, layout, signed = sys.argv[2:]
        run_client(host, int(port), user, key_file, int(announced), layout, int(signed))
    elif sys.argv[1] == "old-server":
        run_old_server(sys.argv[2])
    else:
        sys.exit(f"unknown mode {sys.argv[1]}")
