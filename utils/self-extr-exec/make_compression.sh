#!/usr/bin/env bash

# Put this script near compressor and decompressor

strip decompressor

# TODO use env variables
./compressor ../../programs/clickhouse decompressor
 