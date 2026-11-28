#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--echo` prints every query before it runs, and on a terminal it prints it highlighted.
# `highlighted` (src/Client/ClientBaseHelpers.h) parses the text it is about to print, with
# `max_parser_depth` and `max_parser_backtracks`.  That parse also reports which identifiers stand
# under the cursor, and the client underlines every identifier of that name.  Each pair of matching
# parentheses gets a color of its own, the point where the parse stopped turns red, and the test
# prints every colored run as {class:text}.

python3 - "$CLICKHOUSE_LOCAL" <<'PYEOF'
import fcntl, os, pty, re, struct, sys, termios

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
        for fd in (0, 1, 2): os.dup2(slave, fd)
        if slave > 2: os.close(slave)
        os.execvp('sh', ['sh', '-c', LOCAL + ' ' + args])
    os.close(slave)
    out, chunk = b'', b' '
    try:
        while chunk:
            chunk = os.read(master, 65536); out += chunk
    except OSError:  # The terminal was closed because the child exited.
        pass
    os.close(master)
    status = os.waitpid(pid, 0)[1]
    lines = out.decode('utf-8', 'replace').replace('\r', '').split('\n')
    # Output of a child that did not finish cleanly says nothing, so the status decides.
    if not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0:
        sys.exit('client did not exit cleanly (status %d), output:\n%s' % (status, '\n'.join(lines)))
    return lines

# The colors of the parser-based highlighter: the `type_to_color` map of `highlight` and BRIGHTRED.
CLASSES = {'\x1b[0;1m': 'keyword', '\x1b[0;22;36m': 'identifier',  # bold(DEFAULT), CYAN
           '\x1b[0;22;33m': 'function', '\x1b[0;38;5;40m': 'number',  # BROWN, rgb666(0, 4, 0)
           '\x1b[0;91m': 'error'}
# The parenthesis colors, each followed by its brighter shade: two neighbours are one test class.
SHADES = [(4, 2, 3), (5, 3, 4), (4, 1, 1), (5, 2, 2), (4, 3, 1), (5, 4, 2), (4, 4, 1), (5, 5, 2),
          (1, 4, 1), (2, 5, 2), (1, 4, 4), (2, 5, 5), (2, 1, 4), (3, 2, 5), (4, 1, 4), (5, 2, 5)]
for position, (red, green, blue) in enumerate(SHADES):
    CLASSES['\x1b[0;38;5;%dm' % (16 + 36 * red + 6 * green + blue)] = 'parentheses %d' % (position // 2)
ESCAPE = re.compile(r'\x1b\[[0-9;]*m')

def runs(text):  # The text as (color escape or None, text) pairs, in the order they are printed.
    out, color, pos = [], None, 0
    for match in ESCAPE.finditer(text):
        out.append((color, text[pos:match.start()]))  # \x1b[0m goes back to the default color.
        pos, color = match.end(), None if match.group(0) == '\x1b[0m' else match.group(0)
    return out + [(color, text[pos:])]

def marked(text):  # Every colored run as {class:text}; the default color is kept as it is.
    out = []
    for color, run in runs(text):
        if color is None:
            out.append(run)
            continue
        params = color[2:-1].split(';')
        underlined = len(params) > 1 and params[-1] == '4'
        base = '\x1b[%sm' % ';'.join(params[:-1]) if underlined else color
        out.append('{%s:%s}' % (CLASSES.get(base, 'other') + (' underlined' * underlined), run))
    return ''.join(out)

def pairs(text):  # The two colors of each pair of matching round brackets, innermost pair first.
    stack, out = [], []
    for color, char in [(c, h) for c, run in runs(text) for h in run if h in '()']:
        if char == '(':
            stack.append(color)
        else:
            out.append((stack.pop(), color))
    return out

VALUE = re.compile(r'[0-9 \t]*')
def value(line):  # The answer; an error is reported as a marker and never printed verbatim.
    return line if VALUE.fullmatch(line) else 'not a value'

# Three queries of one invocation, each echoed and highlighted on its own.
# With `implicit_select` the first token of a query can be an identifier, and the cursor the echo
# reports stands on it, so both `number` are underlined; `numbers` stays a function.  A pair of
# matching parentheses is printed in one color, and two nested pairs in two distinct colors.
answer = on_a_terminal("--max_parser_depth 1000 --echo "
                       "--query 'number, number FROM numbers(1); (SELECT 1); SELECT ((1))'")
print('identifiers echo  %s' % marked(answer[0]))
print('identifiers value %s' % value(answer[1]))
# Asserted per statement: the number of pairs, that both brackets of a pair carry one color, and
# that no two pairs of a statement share a color.  The echoed cursor also picks one pair for the
# brighter shade of its color; one class above covers both shades, and the shade stays out of the
# assertion, because which pair of a statement it lands on is unsettled behaviour.
for line in (2, 4):
    print('parentheses echo  %s' % marked(answer[line]))
    print('parentheses value %s' % value(answer[line + 1]))
    found = pairs(answer[line])
    print('parentheses pairs %d matching %s distinct %s'
          % (len(found), all(a == b for a, b in found), len({a for a, _ in found}) == len(found)))
# The highlighter reads at most 10000 bytes of a query, so that the client stays fast on a query too
# long to read anyway.  A longer query is echoed in full and runs, but its colors stop at the limit
# and the last of them is the red mark of the parse error.  `length` returns the padding size.
long_line = on_a_terminal("--max_parser_depth 1000 --echo --query \"SELECT length('%s')\"" % ('a' * 10200))
colored = [color for color, run in runs(long_line[0]) if color is not None]
last = long_line[0].rindex('\x1b[')  # The byte offset of the last color escape of the echoed line.
print('limit echo classes %s, colored runs %d'
      % (', '.join(CLASSES.get(color, 'other') for color in colored[:3]), len(colored)))
print('limit echo bytes %d, colors stop before the limit %s'
      % (len(ESCAPE.sub('', long_line[0])), len(ESCAPE.sub('', long_line[0][:last])) < 10000))
print('limit value %s' % value(long_line[1]))
PYEOF
