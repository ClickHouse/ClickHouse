#!/bin/sh

./compressor < compressor > compressor.snp
./compressor -d < compressor.snp > compressor2
cmp compressor compressor2 && echo "Ok." || echo "Fail."
