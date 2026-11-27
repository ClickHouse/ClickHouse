#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--echo` prints every query before it runs, and on a terminal it prints it highlighted.
# Highlighting is best effort: `highlighted` (src/Client/ClientBaseHelpers.h) parses the text it is
# about to print, with `max_parser_depth` and `max_parser_backtracks`, and prints it unchanged when
# that parse does not finish.  The parse also reports which identifiers stand under the cursor, and
# the client underlines every identifier of that name.  Each pair of matching parentheses gets a
# color of its own ("highlighting of matching parentheses in distinct colors", `clickhouse format
# --help`).  Every colored run is printed below as {class:text}: the test asserts token classes.

python3 - "$CLICKHOUSE_LOCAL" <<'PYEOF'
import fcntl, os, pty, re, struct, subprocess, sys, termios

# Every invocation pins the three settings that decide what the highlighter is able to parse.
LOCAL = sys.argv[1] + " --implicit_select 1 --max_parser_backtracks 1000000"

def on_a_terminal(args):  # A terminal on stdout, because only then is the query highlighted.
    master, slave = pty.openpty()
    # The window size is fixed before the child reads it: the output must not depend on the host.
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
    out, chunk = b'', b' '
    try:
        while chunk:
            chunk = os.read(master, 65536)
            out += chunk
    except OSError:  # The terminal was closed because the child exited.
        pass
    os.close(master)
    status = os.waitpid(pid, 0)[1]
    lines = out.decode('utf-8', 'replace').replace('\r', '').split('\n')
    # Output of a child that did not finish cleanly says nothing, so the status decides.
    if not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0:
        sys.exit('client did not exit cleanly (status %d), output:\n%s' % (status, '\n'.join(lines)))
    return lines

# The colors of the parser-based highlighter, from the `type_to_color` map of `highlight`.
CLASSES = {
    '\x1b[0;1m': 'keyword',         # bold(DEFAULT)
    '\x1b[0;22;36m': 'identifier',  # CYAN
    '\x1b[0;22;33m': 'function',    # BROWN
    '\x1b[0;38;5;40m': 'number',    # rgb666(0, 4, 0)
}
# The eight colors of the parentheses, each followed by the brighter shade `bright_colormap` maps
# it to, in the order of the source: two neighbours of this list are one class of the test.
SHADES = [(4, 2, 3), (5, 3, 4), (4, 1, 1), (5, 2, 2), (4, 3, 1), (5, 4, 2), (4, 4, 1), (5, 5, 2),
          (1, 4, 1), (2, 5, 2), (1, 4, 4), (2, 5, 5), (2, 1, 4), (3, 2, 5), (4, 1, 4), (5, 2, 5)]
for position, (red, green, blue) in enumerate(SHADES):
    CLASSES['\x1b[0;38;5;%dm' % (16 + 36 * red + 6 * green + blue)] = 'parentheses %d' % (position // 2)

ESCAPE = re.compile(r'\x1b\[[0-9;]*m')

def marked(text):
    out, color, pos = [], None, 0  # Every colored run as {class:text}; default color kept as it is.
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

VALUE = re.compile(r'[0-9 \t]*')
BETWEEN = 'SELECT 1 BETWEEN 2 AND 3'

def value(line):  # The answer; an error is reported as a marker and never printed verbatim.
    return line if VALUE.fullmatch(line) else 'not a value'

def parses(depth):
    return subprocess.run('%s --max_parser_depth %d --query "%s" > /dev/null 2>&1'
                          % (LOCAL, depth, BETWEEN), shell=True).returncode == 0

low, high = 10, 25  # The smallest depth the query needs; every deeper limit parses it as well.
while low < high:
    middle = (low + high) // 2
    low, high = (low, middle) if parses(middle) else (middle + 1, high)

# `--echo-formatted` prints the formatted query, and the expansion of BETWEEN repeats its first
# argument inside two comparisons, so the formatted text needs a deeper parse than the text typed:
# at the smallest depth the typed query parses at, its echo is plain, and a generous depth
# highlights it.  The query itself answers in both cases.
for depth, name in ((low, 'least   '), (1000, 'generous')):
    answer = on_a_terminal("--max_parser_depth %d --echo-formatted --echo --query '%s'" % (depth, BETWEEN))
    print('%s echo  %s' % (name, marked(answer[1])))
    print('%s value %s' % (name, value(answer[3])))

# Three queries of one invocation, each echoed and highlighted on its own, to spare two starts.
# With `implicit_select` the first token of a query can be an identifier, and the cursor the echo
# reports stands on it, so both `number` are underlined; `numbers` stays a function.  A pair of
# matching parentheses is printed in one color, wherever in the query the pair stands.
answer = on_a_terminal("--max_parser_depth 1000 --echo "
                       "--query 'number, number FROM numbers(1); (SELECT 1); SELECT (1)'")
print('identifiers echo  %s' % marked(answer[0]))
print('identifiers value %s' % value(answer[1]))
for line in (2, 4):
    print('parentheses echo  %s' % marked(answer[line]))
    print('parentheses value %s' % value(answer[line + 1]))
PYEOF
