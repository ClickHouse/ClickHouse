#!/bin/sh

./compressor < compressor > compressor.qlz
./compressor -d < compressor.qlz > compressor2
cmp compressor compressor2 && echo "Ok." || echo "Fail."
