#!/usr/bin/env bash

# Put this script near compressor and decompressor

cp decompressor clickhouse
strip clickhouse

# TODO use env variables
./compressor ../../programs/clickhouse clickhouse
 