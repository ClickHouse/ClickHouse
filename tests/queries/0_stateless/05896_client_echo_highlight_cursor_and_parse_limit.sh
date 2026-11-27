#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--echo` prints every query before it runs, and on a terminal it prints it highlighted.
# Highlighting is best effort: `highlighted` (src/Client/ClientBaseHelpers.h) parses the text it is
# about to print, with `max_parser_depth` and `max_parser_backtracks`, and prints the text unchanged
# when that parse does not finish.  With `--echo-formatted` the printed text is the formatted query,
# which can need a deeper parse than the query the user typed.
# The parse also reports which identifiers stand under the cursor, and the client underlines all
# identifiers of that name.  An echo reports the cursor at position 0, so an `implicit_select` query
# that starts with an identifier selects them.  Each pair of matching parentheses gets a color of
# its own ("highlighting of matching parentheses in distinct colors", `clickhouse format --help`).
#
# Every colored run is printed as {class:text}, so that the test asserts the visible token classes
# and not the escape sequences.  The two shades of one parenthesis color are reported as one class:
# both mark the same pair, and which of them is used depends on the cursor position alone.

python3 - "$CLICKHOUSE_LOCAL" <<'PYEOF'
import fcntl, os, pty, re, struct, sys, termios

# Every invocation pins the three settings that decide what the highlighter is able to parse.
LOCAL = sys.argv[1] + " --implicit_select 1 --max_parser_backtracks 1000000"

def on_a_terminal(args):
    """Run clickhouse-local with a terminal on stdout, because only then is the query highlighted."""
    master, slave = pty.openpty()
    # Fix the window size before the child reads it, so the output does not depend on the host.
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
    pid = os.fork()
    if pid == 0:
        os.close(master)
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
        for fd in (0, 1, 2):
            os.dup2(slave, fd)
        if slave > 2:
            os.close(slave)
        os.execvp('sh', ['sh', '-c', LOCAL + ' ' + args])
    os.close(slave)
    out = b''
    while True:
        try:
            chunk = os.read(master, 65536)
        except OSError:  # The terminal was closed because the child exited.
            break
        if not chunk:
            break
        out += chunk
    os.close(master)
    os.waitpid(pid, 0)
    return out.decode('utf-8', 'replace').replace('\r', '').split('\n')

# The colors of the parser-based highlighter, from the `type_to_color` map of `highlight`.
CLASSES = {
    '\x1b[0;1m': 'keyword',         # bold(DEFAULT)
    '\x1b[0;22;36m': 'identifier',  # CYAN
    '\x1b[0;22;33m': 'function',    # BROWN
    '\x1b[0;38;5;40m': 'number',    # rgb666(0, 4, 0)
}
# The eight colors of the parentheses, each with the brighter shade `bright_colormap` maps it to.
SHADES = [((4, 2, 3), (5, 3, 4)), ((4, 1, 1), (5, 2, 2)), ((4, 3, 1), (5, 4, 2)), ((4, 4, 1), (5, 5, 2)),
          ((1, 4, 1), (2, 5, 2)), ((1, 4, 4), (2, 5, 5)), ((2, 1, 4), (3, 2, 5)), ((4, 1, 4), (5, 2, 5))]
for index, shades in enumerate(SHADES):
    for red, green, blue in shades:
        CLASSES['\x1b[0;38;5;%dm' % (16 + 36 * red + 6 * green + blue)] = 'parentheses %d' % index

ESCAPE = re.compile(r'\x1b\[[0-9;]*m')

def marked(text):
    """Every colored run as {class:text}; text in the default color is kept as it is."""
    out, color, pos = [], None, 0
    for match in ESCAPE.finditer(text):
        run = text[pos:match.start()]
        out.append('{%s:%s}' % (color, run) if color else run)
        pos = match.end()
        escape = match.group(0)
        if escape == '\x1b[0m':  # Back to the default color.
            color = None
        else:
            params = escape[2:-1].split(';')
            underlined = len(params) > 1 and params[-1] == '4'
            base = '\x1b[%sm' % ';'.join(params[:-1]) if underlined else escape
            color = CLASSES.get(base, 'other') + (' underlined' if underlined else '')
    return ''.join(out) + text[pos:]

# The query as typed parses at depth 16, but the formatted query that `--echo-formatted` prints is
# an expansion of BETWEEN that needs a deeper parse, so its highlighting is skipped.  The query
# itself is unaffected and still answers.  At depth 30 the same text is highlighted.
for depth in (16, 30):
    answer = on_a_terminal("--max_parser_depth %d --echo-formatted --echo --query "
                           "'SELECT 1 BETWEEN 2 AND 3'" % depth)
    print('depth %d echo  %s' % (depth, marked(answer[1])))
    print('depth %d value %s' % (depth, answer[3]))

# The echo reports the cursor at position 0; with `implicit_select` the first token of the query can
# be an identifier, and then every identifier of that name is underlined.  `numbers` is a function
# and keeps its own color.
answer = on_a_terminal("--max_parser_depth 1000 --echo --query 'number, number FROM numbers(1)'")
print('identifiers echo  %s' % marked(answer[0]))
print('identifiers value %s' % answer[1])

# A pair of matching parentheses is printed in one color, wherever in the query the pair stands.
for query in ('(SELECT 1)', 'SELECT (1)'):
    answer = on_a_terminal("--max_parser_depth 1000 --echo --query '%s'" % query)
    print('parentheses echo  %s' % marked(answer[0]))
    print('parentheses value %s' % answer[1])
PYEOF
