#!/usr/bin/env bash

# A pooled pipe command that closes stdout cannot be reused. Its exit status is checked within
# `command_termination_timeout`, after closing stdin so a worker waiting for EOF can exit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function pipe_function()
{
    # name, type, the options that make it what it is, the command
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>$3<command>$4</command></function>"
}

{
    pipe_function pipe_pool_lingers executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>1</command_termination_timeout>" "pipe_udf.py --first-row-only --no-newline --close-stdout --linger"
    pipe_function pipe_pool_lingers_no_exit_check executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>1</command_termination_timeout><check_exit_code>0</check_exit_code>" "pipe_udf.py --first-row-only --no-newline --close-stdout --linger"
    pipe_function pipe_pool_lingers_no_grace executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>0</command_termination_timeout>" "pipe_udf.py --first-row-only --no-newline --close-stdout --linger"
    pipe_function pipe_pool_short_answer executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>20</command_termination_timeout>" "pipe_udf.py --first-row-only --close-stdout --read-to-eof"
    pipe_function pipe_pool_close_stdout_wait_stdin executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>20</command_termination_timeout>" "pipe_udf.py --first-row-only --close-stdout --read-to-eof"
} | shm_functions

echo "--- a pooled worker that closed its stdout and lingers fails the query under the exit check"
# It cannot go back to the pool, so its exit status is read then - and there is none within the
# budget, which is not a passing one. Without the check the answer stands, and the worker is replaced.
shm_local "
    SELECT pipe_pool_lingers(1);
    SELECT pipe_pool_lingers_no_exit_check(1);
    SELECT pipe_pool_lingers_no_exit_check(2);
"
shm_output_contains "closed its stdout but did not exit within command_termination_timeout"

echo "--- with no grace, such a worker fails the query at once and frees its slot"
shm_local "
    SELECT pipe_pool_lingers_no_grace(1);
    SELECT pipe_pool_lingers_no_grace(2);
"
shm_output_contains "did not exit within command_termination_timeout"

echo "--- a pooled worker that is not going back to the pool sees the end of its stdin first"
# Both commands close their stdout and read their stdin to the end before exiting, the way a pooled
# command exits. One answered short and fails the query for that - not for an exit status it could
# only have given after \`command_termination_timeout\`; the other answered in full and passes.
shm_local "
    SELECT pipe_pool_short_answer(number) FROM numbers(3);
    SELECT pipe_pool_close_stdout_wait_stdin(1);
    SELECT pipe_pool_close_stdout_wait_stdin(2);
"
shm_output_contains "did not exit within command_termination_timeout"
