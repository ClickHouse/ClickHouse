#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The shared-memory transport waits for a successful exit when `check_exit_code` is enabled,
# even beyond `command_termination_timeout`. A command that stops answering must still respect
# `command_read_timeout`, whether its stderr is closed or keeps producing output.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    shm_function shm_lingers executable "<command_termination_timeout>1</command_termination_timeout><shared_memory_size>4096</shared_memory_size>" "shm_udf.py --linger 2"
    shm_function shm_lingers_no_exit_check executable \
        "<command_termination_timeout>1</command_termination_timeout><check_exit_code>0</check_exit_code><shared_memory_size>4096</shared_memory_size>" "shm_udf.py --linger 2"
    shm_function shm_quiet_stderr_stalls executable "<command_read_timeout>2000</command_read_timeout><command_termination_timeout>0</command_termination_timeout><shared_memory_size>4096</shared_memory_size>" "shm_udf_broken.py --stall quiet"
    shm_function shm_chatty_stderr_stalls executable \
        "<command_read_timeout>2000</command_read_timeout><command_termination_timeout>0</command_termination_timeout><stderr_reaction>none</stderr_reaction><shared_memory_size>4096</shared_memory_size>" "shm_udf_broken.py --stall chatty"
} | shm_functions

echo "--- a command slow to exit is waited for"
# On stdin EOF it exits successfully only after its \`command_termination_timeout\`.
shm_local "
    SELECT shm_lingers(1);
    SELECT shm_lingers_no_exit_check(1);
"

echo "--- a command that stops answering times out"
# One closes its stderr first, which then polls as a hangup forever; the other keeps writing to
# stderr every 50 ms. Neither may stop \`command_read_timeout\` from firing. Neither exits on stdin EOF
# either, so `command_termination_timeout` is zero: waiting for their exit cannot help this check.
shm_local "
    SELECT shm_quiet_stderr_stalls(1) FORMAT Null;
    SELECT shm_chatty_stderr_stalls(1) FORMAT Null;
"
shm_log_contains "Pipe read timeout exceeded"
