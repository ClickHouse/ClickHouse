#!/usr/bin/env python3

import grpc_tools  # pip3 install grpcio-tools

import os
import subprocess


script_dir = os.path.dirname(os.path.realpath(__file__))
dest_dir = script_dir
src_dir = os.path.abspath(os.path.join(script_dir, "../../../src/Server/grpc_protos"))
src_filename = "clickhouse_grpc.proto"


def generate():
    cmd = [
        "python3",
        "-m",
        "grpc_tools.protoc",
        "-I" + src_dir,
        "--python_out=" + dest_dir,
        "--grpc_python_out=" + dest_dir,
        os.path.join(src_dir, src_filename),
    ]
    subprocess.run(cmd)


if __name__ == "__main__":
    generate()
