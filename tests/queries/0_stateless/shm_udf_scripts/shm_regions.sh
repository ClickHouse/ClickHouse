#!/usr/bin/env bash

# The shared-memory regions the parent process - the `clickhouse-local` that runs this as an
# `executable` table function - holds right now, as `inode size committed_bytes` of its `memfd`
# descriptors. A region has no name in any filesystem, so the parent's descriptor is the only place
# it can be seen from. The inode tells one region from another; the size is the length of the file;
# `st_blocks` (in 512-byte units) is how much of it is backed by pages. A command's copy of the
# descriptor lives in the command's process, so every region is listed once.

for fd in /proc/"$PPID"/fd/*; do
    if [ "$(readlink "$fd")" = "/memfd:clickhouse_udf_shm (deleted)" ]; then
        stat -L -c "%i %s %b" "$fd" | awk '{ print $1 "\t" $2 "\t" $3 * 512 }'
    fi
done
