/// Drive the standalone WebAssembly SQL parser and check that it parses, formats, and reports
/// structured results (`ch_parse` / `ch_format_json`).
import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';
import { Worker } from 'node:worker_threads';

const wasi = new WASI({ version: 'preview1', args: [], env: {}, returnOnExit: true });
const bytes = await readFile(process.argv[2] ?? 'tmp/wasmexp/parser_stripped.wasm');
const { instance } = await WebAssembly.instantiate(bytes, wasi.getImportObject());
wasi.initialize(instance);

const { memory, ch_features, ch_alloc, ch_free, ch_format, ch_parse, ch_format_json, ch_result_data, ch_result_size } = instance.exports;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const FEATURE_FORMAT = 1, FEATURE_DCL = 2, FEATURE_AST_JSON = 4;
const features = ch_features();
const canFormat = !!(features & FEATURE_FORMAT);
const hasDcl = !!(features & FEATURE_DCL);
const hasAstJson = !!(features & FEATURE_AST_JSON);

function call(sql, entry) {
    const bytes = encoder.encode(sql);
    const ptr = ch_alloc(bytes.length);
    new Uint8Array(memory.buffer, ptr, bytes.length).set(bytes);
    const ok = entry(ptr, bytes.length);
    const out = decoder.decode(new Uint8Array(memory.buffer, ch_result_data(), ch_result_size()).slice());
    ch_free(ptr);
    return { ok: !!ok, out };
}

function format(sql, oneLine = 0) {
    if (!canFormat) {
        /// No formatter: drive the same cases through ch_parse and show the message from its JSON.
        const r = call(sql, ch_parse);
        return { ok: r.ok, out: r.ok ? sql : (JSON.parse(r.out).error?.message ?? r.out) };
    }
    return call(sql, (ptr, len) => ch_format(ptr, len, oneLine));
}

const cases = [
    'select 1',
    "SELECT a, b FROM t WHERE x > 1 AND y IN (1,2,3) GROUP BY a HAVING count() > 2 ORDER BY b DESC LIMIT 10",
    "CREATE TABLE t (a UInt64, b String DEFAULT 'x', c Nullable(Decimal(10,2))) ENGINE = MergeTree ORDER BY a",
    "SELECT sum(x) OVER (PARTITION BY k ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM u",
    "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 5) SELECT * FROM cte",
    "ALTER TABLE t ADD COLUMN d Array(Tuple(UInt8, String)) AFTER c",
    "SELECT * FROM t1 ANY LEFT JOIN t2 USING (id) SETTINGS max_threads = 4",
    "INSERT INTO t (a,b) VALUES (1,'x')",
    "SYSTEM DROP REPLICA 'r' FROM ZKPATH '/clickhouse/tables/x//'",
    'SELECT 1 +',                     // expected to fail
    'SELCT 1',                        // expected to fail
    // Reported by throwing from the parser, which has no unwinding here: it must come back as an
    // error, and the module must stay usable afterwards - see __cxa_throw in wasm_runtime.cpp.
    'SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM t',
    'SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t',
    'SELECT 1 + 2',                   // the parse after a throw must still work
];

const expectedToFail = new Set([
    'SELECT 1 +',
    'SELCT 1',
    'SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM t',
    'SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t',
]);

/// Only a build with DCL accepts these.
const dclCases = [
    "CREATE USER u IDENTIFIED WITH sha256_password BY 'p' HOST IP '192.168.0.0/16'",
    "GRANT SELECT(a, b) ON db.tbl TO u WITH GRANT OPTION",
    "SHOW GRANTS FOR u",
];
cases.push(...dclCases);

let pass = 0, total = 0;
for (const sql of cases) {
    total++;
    const r = format(sql, 1);
    const expectFail = expectedToFail.has(sql) || (!hasDcl && dclCases.includes(sql));
    const good = expectFail ? !r.ok : r.ok;
    pass += good ? 1 : 0;
    console.log(`${good ? 'ok  ' : 'FAIL'} ${r.ok ? '' : '[error] '}${r.out.replace(/\n/g, ' ').slice(0, 110)}`);
}
if (canFormat)
    console.log(`\n--- multi-line formatting ---\n${format(cases[1], 0).out}`);

/// --- ch_parse: a JSON document with the AST, the highlights, and the error -------------------

console.log('\n--- ch_parse / ch_format_json ---');

function check(name, condition) {
    total++;
    pass += condition ? 1 : 0;
    console.log(`${condition ? 'ok  ' : 'FAIL'} ${name}`);
}

/// The result of ch_parse must be a JSON document in every outcome.
function parsed(sql) {
    const r = call(sql, ch_parse);
    try {
        return { ok: r.ok, doc: JSON.parse(r.out) };
    } catch {
        return { ok: r.ok, doc: null, raw: r.out };
    }
}

{
    const r = parsed('SELECT 1');
    check('ch_parse ok for SELECT 1', r.ok && r.doc !== null);
    check('highlights: SELECT is a keyword at [0, 6)', !!r.doc?.highlights?.some(
        h => h.begin === 0 && h.end === 6 && h.type === 'keyword'));
    check('highlights: 1 is a number at [7, 8)', !!r.doc?.highlights?.some(
        h => h.begin === 7 && h.end === 8 && h.type === 'number'));
    check('no error reported', r.doc?.error === undefined);
    if (hasAstJson) {
        check('ast is present and typed', typeof r.doc?.ast?.type === 'string');
        const roundtrip = call(JSON.stringify(r.doc.ast), (ptr, len) => ch_format_json(ptr, len, 1));
        check('ch_format_json round-trips the ast', roundtrip.ok && roundtrip.out === format('SELECT 1', 1).out);
    } else {
        check('no ast in this build', r.doc?.ast === undefined);
    }
}

{
    const r = parsed('SELECT\n1 +');
    check('ch_parse fails for SELECT\\n1 +', !r.ok && r.doc !== null);
    check('error message says syntax error', /Syntax error/.test(r.doc?.error?.message ?? ''));
    check('error is at the end of the input', r.doc?.error?.begin === 10 && r.doc?.error?.end === 10);
    check('error line and column are 1-based', r.doc?.error?.line === 2 && r.doc?.error?.column === 4);
    check('expected variants are reported', Array.isArray(r.doc?.error?.expected) && r.doc.error.expected.length > 0);
    check('highlights cover the parsed prefix', !!r.doc?.highlights?.some(
        h => h.begin === 0 && h.end === 6 && h.type === 'keyword'));
}

{
    /// A lexical error, the shape of an editor's half-typed query: the parsed prefix must still be
    /// highlighted, so the coloring does not blink off while the user types the closing quote.
    const r = parsed("SELECT 1, 'abc");
    check('ch_parse fails for an unclosed string', !r.ok && r.doc !== null);
    check('the message names the lexical error', /not closed/.test(r.doc?.error?.message ?? ''));
    check('the error points at the unclosed literal', r.doc?.error?.begin === 10 && r.doc?.error?.end === 14);
    check('highlights cover the prefix of a lexical error', !!r.doc?.highlights?.some(
        h => h.begin === 0 && h.end === 6 && h.type === 'keyword'));
    check('...including the part after the keyword', !!r.doc?.highlights?.some(
        h => h.begin === 7 && h.end === 8 && h.type === 'number'));
}

{
    /// Reported by throwing: no error token, but the message and the highlights must be there.
    const r = parsed('SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM t');
    check('ch_parse reports a thrown error', !r.ok && /UNBOUNDED/.test(r.doc?.error?.message ?? ''));
    check('highlights survive a throw', (r.doc?.highlights?.length ?? 0) > 0);
    check('ch_parse works after a throw', parsed('SELECT 1 + 2').ok);
}

{
    const r = parsed('');
    check('ch_parse reports an empty query', !r.ok && /Empty query/.test(r.doc?.error?.message ?? ''));
}

{
    /// The parser has no stack check of its own below `MAX_PARSER_DEPTH`, so the stack must hold
    /// whatever that depth admits. With a 64 KiB stack, 51 nested parentheses overflowed it, which
    /// traps and leaves the instance unusable. Past the depth, the answer is an error.
    const nest = n => `SELECT ${'('.repeat(n)}1${')'.repeat(n)}`;
    check('ch_parse accepts 190 nested parentheses', parsed(nest(190)).ok);
    const tooDeep = parsed(nest(100000));
    check('ch_parse rejects 100000 nested parentheses by depth',
        !tooDeep.ok && /Maximum parse depth/.test(tooDeep.doc?.error?.message ?? ''));
    const subqueries = n => `SELECT * FROM ${'(SELECT * FROM '.repeat(n)}t${')'.repeat(n)}`;
    check('ch_parse accepts 90 nested subqueries', parsed(subqueries(90)).ok);
    check('ch_parse rejects 10000 nested subqueries by depth',
        /Maximum parse depth/.test(parsed(subqueries(10000)).doc?.error?.message ?? ''));
    check('ch_parse works after the deep queries', parsed('SELECT 1 + 2').ok);
}

if (hasAstJson) {
    /// A query can parse and still have no JSON representation; "ast" is then null with a reason.
    const r = parsed("INSERT INTO t (a,b) VALUES (1,'x')");
    check('INSERT with inline data parses', r.ok);
    check('...but has a null ast with a reason', r.doc?.ast === null && /inline data/.test(r.doc?.ast_error ?? ''));

    if (hasDcl) {
        const grant = parsed('GRANT SELECT ON db.tbl TO u');
        check('GRANT parses with a null ast and a reason', grant.ok && grant.doc?.ast === null
            && typeof grant.doc?.ast_error === 'string');
    }

    /// Hostile input to ch_format_json must come back as an error, never stop the module.
    const malformed = call('this is not JSON', (ptr, len) => ch_format_json(ptr, len, 1));
    check('ch_format_json rejects malformed JSON', !malformed.ok && malformed.out.length > 0);
    const unknown = call('{"type":"Nonsense"}', (ptr, len) => ch_format_json(ptr, len, 1));
    check('ch_format_json rejects an unknown node type', !unknown.ok && /Nonsense/.test(unknown.out));
    const array = call('[1, 2, 3]', (ptr, len) => ch_format_json(ptr, len, 1));
    check('ch_format_json rejects a non-object document', !array.ok);
    const nested = call(`{"type":"Function","children":${'['.repeat(100000)}${']'.repeat(100000)}}`,
        (ptr, len) => ch_format_json(ptr, len, 1));
    check('ch_format_json rejects deeply nested JSON', !nested.ok && nested.out.length > 0);
    check('ch_parse works after ch_format_json errors', parsed('SELECT 1 + 2').ok);

    /// Multi-line formatting through the JSON path matches the direct path.
    const sql = cases[1];
    const viaJson = call(JSON.stringify(parsed(sql).doc.ast), (ptr, len) => ch_format_json(ptr, len, 0));
    check('ch_format_json multi-line matches ch_format', viaJson.ok && viaJson.out === format(sql, 0).out);

    /// The producer and the consumer of the AST JSON hold to the same limits: whatever `ch_parse`
    /// reports as an "ast", `ch_format_json` reads back.
    const wide = `SELECT ${Array(1000).fill('1').join(', ')}`;
    const wideParsed = parsed(wide);
    check('a wide query parses with an ast', wideParsed.ok && !!wideParsed.doc?.ast);
    const wideBack = call(JSON.stringify(wideParsed.doc.ast), (ptr, len) => ch_format_json(ptr, len, 1));
    check('ch_format_json reads a wide ast back', wideBack.ok);

    /// Structured literals are where the two sides count differently - `Array`, `Tuple` and nested
    /// values of them are one `ASTLiteral` each, whatever their width - so the ordinary case is
    /// pinned as well: they round-trip, the read-back does not turn them away.
    const structured = "SELECT [1, 2, 3], (4, 'five'), [[1], [2, 3]]";
    const structuredParsed = parsed(structured);
    check('a query of structured literals has an ast', structuredParsed.ok && !!structuredParsed.doc?.ast);
    const structuredBack = call(JSON.stringify(structuredParsed.doc?.ast), (ptr, len) => ch_format_json(ptr, len, 1));
    check('ch_format_json round-trips structured literals',
        structuredBack.ok && structuredBack.out === format(structured, 1).out);

    /// Subqueries a few levels deep. Their AST JSON used to come back null, with "Stack size too
    /// large", while the module had wasm-ld's default 64 KiB stack - see `-z stack-size` in
    /// CMakeLists.txt.
    for (const sql of [
        'SELECT * FROM (SELECT * FROM (SELECT number FROM numbers(10)))',
        "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE country = 'PT')",
        'select * from t where x in (select y from u)',
        'with top as (select user_id, sum(amount) as total from orders group by user_id) select user_id from top',
    ]) {
        const r = parsed(sql);
        check(`subquery has an ast: ${sql.slice(0, 50)}`, r.ok && !!r.doc?.ast);
        const back = call(JSON.stringify(r.doc?.ast), (ptr, len) => ch_format_json(ptr, len, 1));
        check(`...and ch_format_json round-trips it`, back.ok && back.out === format(sql, 1).out);
    }

    /// Real queries of the depth that the stack size is about: a sum over many columns, a
    /// chain of `if`, and a pipeline of derived tables. All of them had a null "ast" with a 64 KiB
    /// stack.
    const sum = n => `SELECT ${Array.from({ length: n }, (_, i) => `revenue_${i + 1}`).join(' + ')} AS total FROM sales`;
    const ifs = n => `SELECT ${'if(x = 0, 0, '.repeat(n)}x${')'.repeat(n)} FROM t`;
    const pipeline = n => {
        let sql = 'SELECT id FROM t0';
        for (let i = 1; i <= n; ++i)
            sql = `SELECT id FROM (${sql}) AS s${i} WHERE id > ${i}`;
        return sql;
    };
    for (const [name, sql] of [
        ['a sum of 30 columns', sum(30)],
        ['25 nested if', ifs(25)],
        ['8 nested derived tables', pipeline(8)],
    ]) {
        const r = parsed(sql);
        check(`${name} has an ast`, r.ok && !!r.doc?.ast);
        const back = call(JSON.stringify(r.doc?.ast), (ptr, len) => ch_format_json(ptr, len, 1));
        check(`...and ch_format_json round-trips it`, back.ok && back.out === format(sql, 1).out);
    }

    /// A longer chain still has its JSON. A much longer one runs into the stack check, which must
    /// answer a null "ast" with the reason - not stop the module - and a tree past the depth limit
    /// is a parse error.
    const chain = n => `SELECT ${Array(n).fill('1').join(' + ')}`;
    const chainParsed = parsed(chain(20));
    check('a chain of 20 terms has an ast', chainParsed.ok && !!chainParsed.doc?.ast);
    const chainBack = call(JSON.stringify(chainParsed.doc?.ast), (ptr, len) => ch_format_json(ptr, len, 1));
    check('...and ch_format_json round-trips it', chainBack.ok && chainBack.out === format(chain(20), 1).out);
    const longChain = parsed(chain(450));
    check('a chain of 450 terms parses, with a null ast because of the stack', longChain.ok
        && longChain.doc?.ast === null && /Stack size too large/.test(longChain.doc?.ast_error ?? ''));
    const pastLimit = parsed(chain(600));
    check('past the depth limit the error is the depth', !pastLimit.ok
        && /too deep/.test(pastLimit.doc?.error?.message ?? ''));
    check('ch_parse works after the stack check', parsed('SELECT 1 + 2').ok);

    /// Past those limits the "ast" is null with a reason - never JSON this module cannot read back.
    /// The first query is over the element budget; the second one fits in the input limit while its
    /// JSON does not, because every quote in the literal is escaped. The third one is under the
    /// budget by AST nodes alone (49905 of them) and over it once the reader counts the values of
    /// the array literal as well, which all live inside a single `ASTLiteral`: only reading the
    /// document back the way `ch_format_json` does catches that one.
    for (const [name, query] of [
        ['an ast over the element limit', `SELECT ${Array(60000).fill('1').join(', ')}`],
        ['an ast whose JSON is over the input limit', `SELECT '${'"'.repeat(900000)}'`],
        ['an ast over the element limit only once its literal is counted',
            `SELECT [${Array(200).fill('1').join(',')}], ${Array(49900).fill('*').join(', ')}`],
    ]) {
        const r = parsed(query);
        const back = r.ok && r.doc?.ast
            ? call(JSON.stringify(r.doc.ast), (ptr, len) => ch_format_json(ptr, len, 1)).ok
            : null;
        check(`${name}: null with a reason, or readable back`,
            !r.ok || (r.doc?.ast === null ? /too big|limit/i.test(r.doc?.ast_error ?? '') : back === true));
    }
}

{
    /// A statement that fails after its keyword committed reports the tree built so far as
    /// "partial_ast", with an `Error` node in the slot of the clause that failed. The "error" is
    /// exactly what a parse without the capture reports. Only a build with AST JSON has it.
    const incomplete = [
        /// The query, the error token, the slot of the `Error` and its range, a slot parsed before it.
        ['SELECT a FROM', [13, 13], 'tables', [13, 13], 'select'],
        ['SELECT a FROM t WHERE', [21, 21], 'where', [21, 21], 'tables'],
        ['SELECT a FROM t ORDER BY', [24, 24], 'order_by', [24, 24], 'tables'],
        /// A broken expression is one `Error` in its clause. It covers the `+`, where the parser
        /// last recorded what it expected, while the error token is the end of the input.
        ['SELECT 1 +', [10, 10], 'select', [9, 10], null],
    ];
    for (const [sql, [errorBegin, errorEnd], slot, [begin, end], before] of incomplete) {
        const r = parsed(sql);
        check(`ch_parse fails for ${sql}`, !r.ok && r.doc?.ast === undefined);
        check('...with the error a parse without the capture reports', r.doc?.error?.message === format(sql, 1).out
            && r.doc?.error?.begin === errorBegin && r.doc?.error?.end === errorEnd);
        if (hasAstJson) {
            const partial = r.doc?.partial_ast;
            check('...and a partial SelectQuery', partial?.type === 'SelectQuery');
            check(`...with an Error in "${slot}" at [${begin}, ${end})`, partial?.[slot]?.type === 'Error'
                && partial[slot].begin === begin && partial[slot].end === end
                && Array.isArray(partial[slot].expected) && partial[slot].expected.length > 0);
            if (before)
                check(`...after the "${before}" that parsed`, typeof partial?.[before]?.type === 'string' && partial[before].type !== 'Error');
        } else {
            check('...and no partial_ast in this build', r.doc?.partial_ast === undefined);
        }
    }

    const complete = parsed('SELECT 1');
    check('SELECT 1 has no partial_ast', complete.ok && complete.doc?.partial_ast === undefined);
    const noKeyword = parsed('SELEC');
    check('SELEC has no partial_ast: no statement keyword committed', !noKeyword.ok && noKeyword.doc?.partial_ast === undefined);

    if (hasAstJson) {
        const partial = parsed('SELECT a FROM').doc?.partial_ast;
        const back = call(JSON.stringify(partial), (ptr, len) => ch_format_json(ptr, len, 1));
        check('ch_format_json rejects a partial_ast', !back.ok && /'Error'/.test(back.out));
    }
}

{
    /// `INSERT`, `CREATE TABLE` and `ALTER` report the same way. Their `Error` is the value of the JSON
    /// key of the slot that failed, or the last element of a column list whose `)` is missing. A
    /// `SELECT` that failed inside one is reported as it is today, on its own.
    const errorsIn = (node, path = '', found = []) => {
        if (Array.isArray(node))
            node.forEach((value, i) => errorsIn(value, `${path}[${i}]`, found));
        else if (node && typeof node === 'object') {
            if (node.type === 'Error')
                found.push({ path, node });
            else
                for (const [key, value] of Object.entries(node))
                    errorsIn(value, path ? `${path}.${key}` : key, found);
        }
        return found;
    };
    /// `JSON.parse` keeps the last of two equal keys; a document with one is wrong all the same.
    const hasDuplicateKeys = text => {
        const stack = [];
        for (let i = 0; i < text.length; ++i) {
            const c = text[i];
            if (c === '"') {
                let j = i + 1;
                while (text[j] !== '"')
                    j += text[j] === '\\' ? 2 : 1;
                const top = stack[stack.length - 1];
                if (top?.keys && top.expectKey) {
                    const key = text.slice(i + 1, j);
                    if (top.keys.has(key))
                        return true;
                    top.keys.add(key);
                    top.expectKey = false;
                }
                i = j;
            }
            else if (c === '{')
                stack.push({ keys: new Set(), expectKey: true });
            else if (c === '[')
                stack.push({});
            else if (c === '}' || c === ']')
                stack.pop();
            else if (c === ',' && stack[stack.length - 1]?.keys)
                stack[stack.length - 1].expectKey = true;
        }
        return false;
    };

    const T = 'CREATE TABLE t (a UInt8)';
    /// The query, then the root type, the path of the `Error` and its range - or null for no partial_ast.
    const statements = [
        ['INSERT INTO t (a,', 'InsertQuery', 'columns.children[1]', 17, 17],
        ['INSERT INTO t (', 'InsertQuery', 'columns', 15, 15],
        ['INSERT INTO t FORMAT', 'InsertQuery', 'format', 20, 20],
        ['INSERT INTO t (a) SELECT 1 FORMAT', 'InsertQuery', 'format', 33, 33],
        ['EXPLAIN AST INSERT INTO t (a) FORMAT', 'InsertQuery', 'format', 36, 36],
        ['WITH x AS (SELECT 1) INSERT INTO t FORMAT', 'InsertQuery', 'format', 41, 41],
        ['INSERT INTO', 'InsertQuery', 'table', 11, 11],
        ['INSERT INTO db.', 'InsertQuery', 'table', 15, 15],
        ['INSERT INTO FUNCTION', 'InsertQuery', 'table_function', 20, 20],
        ["INSERT INTO FUNCTION file('x') PARTITION BY", 'InsertQuery', 'partition_by', 43, 43],
        ['INSERT INTO t FROM INFILE', 'InsertQuery', 'infile', 25, 25],
        ["INSERT INTO t FROM INFILE 'f' COMPRESSION", 'InsertQuery', 'compression', 41, 41],
        ['INSERT INTO t SETTINGS', 'InsertQuery', 'settings_ast', 22, 22],
        ['INSERT INTO t SELECT a FROM', 'SelectQuery', 'tables', 27, 27],
        ['INSERT INTO t SELECT 1 UNION ALL SELECT a FROM', 'SelectQuery', 'tables', 46, 46],
        /// The column alias list after the tables, where a modifier ends the table expression.
        ['SELECT * FROM t FINAL (', 'SelectQuery', 'aliases', 23, 23],
        ['SELECT * FROM t SAMPLE 1 (a', 'SelectQuery', 'aliases', 27, 27],
        ['FROM t FINAL (a', 'SelectQuery', 'aliases', 15, 15],
        /// The modifiers after `GROUP BY` are a failure of `group_by`, also where there is none.
        ['SELECT 1 GROUP BY x WITH', 'SelectQuery', 'group_by', 24, 24],
        ['SELECT 1 GROUP BY x WITH ROLLUP WITH', 'SelectQuery', 'group_by', 36, 36],
        /// A repeated modifier: the `Error` is at it, where the parser last recorded what it expected.
        ['SELECT 1 GROUP BY x WITH TOTALS WITH TOTALS', 'SelectQuery', 'group_by', 37, 37],
        ['SELECT count() FROM t WITH', 'SelectQuery', 'group_by', 26, 26],
        ['SELECT 1 WINDOW', 'SelectQuery', 'window', 15, 15],
        ['SELECT 1 FROM t WINDOW w AS (PARTITION BY', 'SelectQuery', 'window', 41, 41],
        ['SELECT 1 QUALIFY', 'SelectQuery', 'qualify', 16, 16],
        ['SELECT 1 OFFSET', 'SelectQuery', 'limit_offset', 15, 15],
        /// `FETCH` is the limit of the finished query, so a failure anywhere in it is in `limit_length`.
        ['SELECT 1 ORDER BY x FETCH', 'SelectQuery', 'limit_length', 25, 25],
        ['SELECT 1 ORDER BY x FETCH FIRST 5 ROWS', 'SelectQuery', 'limit_length', 38, 38],
        ['SELECT 1 ORDER BY x OFFSET 2 ROWS FETCH NEXT 3', 'SelectQuery', 'limit_length', 46, 46],
        /// Of statements failed at the same place, the innermost: the subquery, not the query around it.
        ['SELECT * FROM (SELECT a FROM', 'SelectQuery', 'tables', 28, 28],
        ['SELECT a FROM t WHERE x IN (SELECT', 'SelectQuery', 'select', 34, 34],
        /// Captured at the column list, which the parser then got past.
        ['INSERT INTO t (SELECT 1 FROM x) VALUES', null],
        /// A missing data source is an `Error` in `select`, the one source `ast` has as a tree.
        ['INSERT INTO t', 'InsertQuery', 'select', 13, 13],
        ['INSERT INTO t (a) VALUE', 'InsertQuery', 'select', 18, 23],
        ['INSERT INTO t SELECT 1 SETTINGS max_threads = 1 FORMAT', 'InsertQuery', 'format', 54, 54],

        ['CREATE TABLE', 'CreateQuery', 'table_ast', 12, 12],
        ['CREATE TABLE t ON CLUSTER', 'CreateQuery', 'cluster', 25, 25],
        ['CREATE TABLE t (', 'CreateQuery', 'columns_list', 16, 16],
        ['CREATE TABLE t (a UInt8,', 'CreateQuery', 'columns_list.columns.children[1]', 24, 24],
        ["CREATE TABLE t (a String DEFAULT 'abc", 'CreateQuery', 'columns_list', 33, 37],
        /// The list does not keep the order of columns and indices, so the whole list is the slot.
        ['CREATE TABLE t (INDEX i a TYPE minmax,', 'CreateQuery', 'columns_list.indices.children[1]', 38, 38],
        ['CREATE TABLE t (a UInt8, INDEX i a TYPE minmax GRANULARITY 1', 'CreateQuery', 'columns_list.indices.children[1]', 60, 60],
        ['CREATE TABLE t (INDEX i a TYPE minmax, a UInt8', 'CreateQuery', 'columns_list.columns.children[1]', 46, 46],
        ['CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0', 'CreateQuery', 'columns_list.constraints.children[1]', 49, 49],
        /// A `PRIMARY KEY` written last is not in a list of `columns_list`, so the whole list is the slot.
        ['CREATE TABLE t (a UInt8, PRIMARY KEY a', 'CreateQuery', 'columns_list', 38, 38],
        [`${T} ENGINE = MergeTree AS`, 'CreateQuery', 'select', 46, 46],
        [`${T} EMPTY AS`, 'CreateQuery', 'as_table_function', 33, 33],
        [`${T} CLONE AS`, 'CreateQuery', 'as_table_function', 33, 33],
        ['CREATE TABLE t AS SELECT a FROM', 'SelectQuery', 'tables', 31, 31],
        [`${T} ENGINE = MergeTree AS SELECT a FROM`, 'SelectQuery', 'tables', 60, 60],
        ['CREATE TABLE t (a UInt8 DEFAULT (SELECT 1 FROM', 'SelectQuery', 'tables', 46, 46],
        /// Storage is optional: it is the slot only when the storage parser itself got further. Each
        /// clause of it is its own key of `storage`.
        [`${T} ENGINE =`, 'CreateQuery', 'storage.engine', 33, 33],
        [`${T} ENGINE`, 'CreateQuery', 'storage.engine', 31, 31],
        [`${T} ORDER BY`, 'CreateQuery', 'storage.order_by', 33, 33],
        /// Both readings hold: the query-level `SETTINGS` fails on the same token.
        [`${T} SETTINGS`, 'CreateQuery', 'storage.settings', 33, 33],
        [`${T} ENGINE = MergeTree() PARTITION BY`, 'CreateQuery', 'storage.partition_by', 58, 58],
        [`${T} ENGINE = MergeTree ORDER BY a TTL`, 'CreateQuery', 'storage.ttl_table', 58, 58],
        [`${T} ENGINE = MergeTree ORDER BY a SAMPLE BY`, 'CreateQuery', 'storage.sample_by', 64, 64],
        [`${T} ENGINE = MergeTree ORDER BY a UNIQUE KEY`, 'CreateQuery', 'storage.unique_key', 65, 65],
        [`${T} ENGINE = MergeTree() ORDER BY tuple() SETTINGS index_granularity =`, 'CreateQuery', 'storage.settings', 91, 91],
        [`${T} ENGINE = MergeTree ORDER BY (a`, 'CreateQuery', 'storage.order_by', 55, 55],
        ['CREATE TABLE t ENGINE =', 'CreateQuery', 'storage.engine', 23, 23],
        ['CREATE TABLE t AS other ENGINE =', 'CreateQuery', 'storage.engine', 32, 32],
        ['CREATE TEMPORARY TABLE t (a UInt8) ENGINE =', 'CreateQuery', 'storage.engine', 43, 43],
        [`${T} BLAH`, null],
        [`${T} COMMENT`, 'CreateQuery', 'comment', 32, 32],
        [`${T} SQL SECURITY`, 'CreateQuery', 'sql_security', 37, 37],
        ['CREATE TABLE t AS other COMMENT', 'CreateQuery', 'comment', 31, 31],
        [`${T} COMMENT 'x' ENGINE =`, null],
        [`${T} ENGINE = MergeTree ORDER BY a,`, null],
        [`${T} ENGINE = MergeTree ORDER BY a >`, null],
        /// The statement parsed up to `remote`: the unmatched `(` is input after it, not a failure in it.
        ['CREATE TABLE t AS remote(', null],
        /// The table function, tried first, got furthest: `x.y.z` as its name, then no `(`.
        ['CREATE TABLE t AS x.y.z', 'CreateQuery', 'as_table_function', 23, 23],
        ['CREATE TABLE t AS db.', 'CreateQuery', 'as_table', 21, 21],
        ["CREATE TABLE t AS remote('h',", 'CreateQuery', 'as_table_function', 29, 29],
        /// After `AS`, nothing parsed: `select`, as for a table with columns.
        ['CREATE TABLE t AS', 'CreateQuery', 'select', 17, 17],
        ['CREATE TABLE t ENGINE = MergeTree AS', 'CreateQuery', 'select', 36, 36],
        [`${T} ENGINE = Memory EMPTY`, 'CreateQuery', 'select', 46, 46],
        ['ATTACH TABLE t FROM', 'CreateQuery', 'attach_from_path', 19, 19],
        ['ATTACH TABLE t AS NOT', 'CreateQuery', 'attach_as_replicated', 21, 21],
        ['CREATE TABLE t TO INNER UUID', 'CreateQuery', 'targets', 28, 28],

        ['CREATE VIEW', 'CreateQuery', 'table_ast', 11, 11],
        ['CREATE VIEW v ON CLUSTER', 'CreateQuery', 'cluster', 24, 24],
        ['CREATE VIEW v', 'CreateQuery', 'select', 13, 13],
        ['CREATE VIEW v AS', 'CreateQuery', 'select', 16, 16],
        ['CREATE VIEW v AS SELECT a FROM', 'SelectQuery', 'tables', 30, 30],
        /// The column aliases, and the columns, as a list that lacks its `)`.
        ['CREATE VIEW v (a,', 'CreateQuery', 'aliases_list.children[1]', 17, 17],
        ['CREATE VIEW v (a Int64) (', 'CreateQuery', 'aliases_list', 25, 25],
        ['CREATE VIEW v (a Int64,', 'CreateQuery', 'columns_list.columns.children[1]', 23, 23],
        /// `REFRESH` is for a materialized view only; its parts are each in their own key.
        ['CREATE VIEW v REFRESH EVERY 1 HOUR', 'CreateQuery', 'refresh_strategy', 14, 14],
        ['CREATE MATERIALIZED VIEW v REFRESH', 'CreateQuery', 'refresh_strategy', 34, 34],
        ['CREATE MATERIALIZED VIEW v REFRESH EVERY', 'CreateQuery', 'refresh_strategy.period', 40, 40],
        ['CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR OFFSET', 'CreateQuery', 'refresh_strategy.offset', 54, 54],
        ['CREATE MATERIALIZED VIEW v REFRESH AFTER 1 HOUR RANDOMIZE FOR', 'CreateQuery', 'refresh_strategy.spread', 61, 61],
        ['CREATE MATERIALIZED VIEW v REFRESH AFTER 1 HOUR DEPENDS ON', 'CreateQuery', 'refresh_strategy.dependencies', 58, 58],
        ['CREATE MATERIALIZED VIEW v REFRESH AFTER 1 HOUR SETTINGS', 'CreateQuery', 'refresh_strategy.settings', 56, 56],
        /// With no schedule, `DEPENDS ON` is what is missing; the spread that parsed is kept.
        ['CREATE MATERIALIZED VIEW v REFRESH RANDOMIZE FOR 1 SECOND', 'CreateQuery', 'refresh_strategy.dependencies', 57, 57],
        ['CREATE MATERIALIZED VIEW v TO', 'CreateQuery', 'targets', 29, 29],
        ['CREATE MATERIALIZED VIEW v TO INNER UUID', 'CreateQuery', 'targets', 40, 40],
        ['CREATE MATERIALIZED VIEW v TO t', 'CreateQuery', 'select', 31, 31],
        /// The storage of a materialized view is the inner engine of its `To` target, as in `ast`.
        ['CREATE MATERIALIZED VIEW v ENGINE =', 'CreateQuery', 'targets.targets[0].inner_engine.engine', 35, 35],
        ['CREATE MATERIALIZED VIEW v ENGINE = Memory POPULATE', 'CreateQuery', 'select', 51, 51],
        /// Optional parts, captured when their parser got past its first token.
        ['CREATE VIEW v COMMENT', 'CreateQuery', 'comment', 21, 21],
        ['CREATE VIEW v (a) DEFINER =', 'CreateQuery', 'sql_security', 27, 27],
        ['CREATE VIEW v (a) SQL SECURITY', 'CreateQuery', 'sql_security', 30, 30],

        ['CREATE DICTIONARY', 'CreateQuery', 'table_ast', 17, 17],
        ['CREATE DICTIONARY d ON CLUSTER', 'CreateQuery', 'cluster', 30, 30],
        ['CREATE DICTIONARY d', 'CreateQuery', 'dictionary_attributes_list', 19, 19],
        ['CREATE DICTIONARY d (a UInt8,', 'CreateQuery', 'dictionary_attributes_list.children[1]', 29, 29],
        ['CREATE DICTIONARY d (a UInt8 DEFAULT', 'CreateQuery', 'dictionary_attributes_list', 36, 36],
        /// Each clause of the definition is its own key of `dictionary`; a broken one is an `Error` there.
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY', 'CreateQuery', 'dictionary.primary_key', 41, 41],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY (a,', 'CreateQuery', 'dictionary.primary_key.children[1]', 45, 45],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a SOURCE(', 'CreateQuery', 'dictionary.source', 51, 51],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a SOURCE(NULL()', 'CreateQuery', 'dictionary.source', 57, 57],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a LAYOUT(', 'CreateQuery', 'dictionary.layout', 51, 51],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a LIFETIME(MIN 1', 'CreateQuery', 'dictionary.lifetime', 58, 58],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a RANGE(', 'CreateQuery', 'dictionary.range', 50, 50],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a SETTINGS(', 'CreateQuery', 'dictionary.dict_settings', 53, 53],
        ['CREATE DICTIONARY d (a UInt8) PRIMARY KEY a LIFETIME(0) COMMENT', 'CreateQuery', 'comment', 63, 63],

        ['ALTER TABLE', 'AlterQuery', 'table_ast', 11, 11],
        ['ALTER TABLE db.', 'AlterQuery', 'table_ast', 15, 15],
        ['ALTER TABLE t', 'AlterQuery', 'command_list', 13, 13],
        /// A command that failed keeps its type and what parsed of it, after the commands before it.
        ['ALTER TABLE t ADD COLUMN', 'AlterQuery', 'command_list.children[0].col_decl', 24, 24],
        ['ALTER TABLE t DROP COLUMN a, ADD COLUMN b UInt8 AFTER', 'AlterQuery', 'command_list.children[1].column', 53, 53],
        ['ALTER TABLE t DROP COLUMN a,', 'AlterQuery', 'command_list.children[1]', 28, 28],
        ['ALTER TABLE t RENAME COLUMN a', 'AlterQuery', 'command_list.children[0].rename_to', 29, 29],
        ['ALTER TABLE t CLEAR COLUMN a IN PARTITION', 'AlterQuery', 'command_list.children[0].partition', 41, 41],
        ['ALTER TABLE t DETACH PART', 'AlterQuery', 'command_list.children[0].partition', 25, 25],
        ["ALTER TABLE t MOVE PART 'p' TO DISK", 'AlterQuery', 'command_list.children[0].move_destination_name', 35, 35],
        ['ALTER TABLE t MOVE PARTITION 1 TO TABLE', 'AlterQuery', 'command_list.children[0].to_table', 39, 39],
        ['ALTER TABLE t ATTACH PARTITION 1 FROM', 'AlterQuery', 'command_list.children[0].from_table', 37, 37],
        ['ALTER TABLE t FETCH PARTITION 1 FROM', 'AlterQuery', 'command_list.children[0].from', 36, 36],
        ['ALTER TABLE t FREEZE WITH NAME', 'AlterQuery', 'command_list.children[0].with_name', 30, 30],
        ['ALTER TABLE t MODIFY COLUMN a REMOVE', 'AlterQuery', 'command_list.children[0].remove_property', 36, 36],
        ['ALTER TABLE t MODIFY COLUMN a ADD ENUM VALUES (', 'AlterQuery', 'command_list.children[0].add_enum_values', 47, 47],
        ['ALTER TABLE t UPDATE a = 1 WHERE', 'AlterQuery', 'command_list.children[0].predicate', 32, 32],
        ['ALTER TABLE t DELETE WHERE a AS x', 'AlterQuery', 'command_list.children[0].predicate', 32, 32],
        ['ALTER TABLE t MODIFY TTL', 'AlterQuery', 'command_list.children[0].ttl', 24, 24],
        ['ALTER TABLE t MODIFY REFRESH EVERY', 'AlterQuery', 'command_list.children[0].refresh.period', 34, 34],
        ['ALTER TABLE t ADD PROJECTION', 'AlterQuery', 'command_list.children[0].projection_decl', 28, 28],
        ['ALTER TABLE t EXECUTE f(', 'AlterQuery', 'command_list.children[0].execute_args', 24, 24],
        ['ALTER TABLE t UNLOCK SNAPSHOT', 'AlterQuery', 'command_list.children[0].snapshot_name', 29, 29],
        ['ALTER DATABASE d MODIFY COMMENT', 'AlterQuery', 'command_list.children[0].comment', 31, 31],
        ['ALTER TABLE t ON CLUSTER', 'AlterQuery', 'cluster', 24, 24],
        ['ALTER DATABASE', 'AlterQuery', 'database_ast', 14, 14],
        ['ALTER DATABASE d ON CLUSTER', 'AlterQuery', 'cluster', 27, 27],
        ['ALTER TABLE t MODIFY QUERY SELECT a FROM', 'SelectQuery', 'tables', 40, 40],
        ['ALTER TABLE t DELETE WHERE x IN (SELECT a FROM', 'SelectQuery', 'tables', 46, 46],
    ];
    for (const [sql, type, path, begin, end] of statements) {
        const raw = call(sql, ch_parse);
        const r = { ok: raw.ok, doc: JSON.parse(raw.out) };
        const partial = r.doc.partial_ast;
        const same = !r.ok && r.doc.ast === undefined && r.doc.error?.message === format(sql, 1).out;
        if (!hasAstJson || type === null) {
            check(`${sql}: fails with no partial_ast`, same && partial === undefined);
            continue;
        }
        const errors = errorsIn(partial);
        check(`${sql}: fails with a partial ${type}, an Error in "${path}" at [${begin}, ${end})`, same
            && partial?.type === type && errors.length === 1 && errors[0].path === path
            && errors[0].node.begin === begin && errors[0].node.end === end && !hasDuplicateKeys(raw.out));
        /// The parse can add to what it expects after the capture: `end of query` after `INSERT ... SELECT`.
        check('...expecting what "error" expects', JSON.stringify(errors[0]?.node.expected) === JSON.stringify(r.doc.error.expected ?? []));
    }

    if (hasAstJson) {
        /// What parsed before the failure is there as in "ast", spelled as `ch_parse` spells it on success.
        const partial = parsed('CREATE TABLE t AS other ENGINE =').doc?.partial_ast;
        check('...with the parts that parsed', partial?.table === 't' && partial?.as_table === 'other');
        const insert = parsed('INSERT INTO t (a, b,').doc?.partial_ast;
        check('...and the columns that parsed before the Error',
            insert?.columns?.children?.map(c => c.name ?? c.type).join() === 'a,b,Error');
        const subquery = parsed('SELECT * FROM (SELECT a FROM').doc?.partial_ast;
        check('...and of a subquery, its own select list', subquery?.select?.children?.[0]?.name === 'a');

        /// Members the success path sets at the end are there as in the "ast" of the whole statement,
        /// and so are the parts of a clause that failed, by their path.
        const at = (tree, path) => path.split('.').reduce((v, k) => v?.[k], tree);
        for (const [prefix, rest, keys] of [
            ["CREATE TABLE t UUID '123e4567-e89b-12d3-a456-426614174000' (a UInt8) ENGINE =", ' MergeTree ORDER BY a',
                ['uuid', 'has_uuid', 'has_uuid_clause']],
            ["CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a COMMENT 'x' AS", ' SELECT 1', ['comment']],
            ['SELECT DISTINCT ON (a) b FROM', ' t', ['limit_by', 'limit_by_length']],
            ['CREATE MATERIALIZED VIEW v TO db.t', ' AS SELECT 1', ['table_ast', 'targets', 'is_materialized_view']],
            ['CREATE MATERIALIZED VIEW v ENGINE = Memory POPULATE', ' AS SELECT 1', ['targets', 'is_populate']],
            [`${T} ENGINE = MergeTree() PARTITION BY a PRIMARY KEY a ORDER BY a SAMPLE BY a TTL a + INTERVAL 1 DAY SETTINGS`,
                ' index_granularity = 1', ['storage.engine', 'storage.partition_by', 'storage.primary_key', 'storage.order_by',
                    'storage.sample_by', 'storage.ttl_table']],
            ['CREATE MATERIALIZED VIEW v ENGINE = MergeTree ORDER BY a SETTINGS', ' index_granularity = 1 AS SELECT 1 AS a',
                ['targets.targets.0.kind', 'targets.targets.0.inner_engine.engine', 'targets.targets.0.inner_engine.order_by']],
            ["CREATE VIEW v (a Int64) (a) DEFINER = u SQL SECURITY DEFINER COMMENT 'c' AS", ' SELECT 1',
                ['columns_list', 'aliases_list', 'sql_security', 'comment', 'is_ordinary_view']],
            ['CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR OFFSET', ' 5 MINUTE AS SELECT 1',
                ['refresh_strategy.schedule_kind', 'refresh_strategy.period']],
            ['CREATE MATERIALIZED VIEW v REFRESH AFTER 1 HOUR RANDOMIZE FOR 1 MINUTE DEPENDS ON a SETTINGS', ' x = 1 AS SELECT 1',
                ['refresh_strategy.period', 'refresh_strategy.spread', 'refresh_strategy.dependencies']],
            ['CREATE DICTIONARY db.d ON CLUSTER c (a UInt8, b String DEFAULT \'x\') PRIMARY KEY a SOURCE(NULL()) LAYOUT(FLAT()) RANGE(',
                'MIN a MAX a) LIFETIME(0)', ['table_ast', 'cluster', 'is_dictionary', 'dictionary_attributes_list',
                    'dictionary.primary_key', 'dictionary.source', 'dictionary.layout']],
            ['ALTER TABLE t DROP COLUMN a, ADD COLUMN b UInt8 AFTER', ' a', ['table_ast', 'command_list.children.0', 'command_list.children.1.command_type',
                'command_list.children.1.col_decl']],
            ["ALTER TABLE t DETACH PART", " 'p'", ['command_list.children.0.command_type', 'command_list.children.0.part', 'command_list.children.0.detach']],
            ['ALTER TABLE t CLEAR COLUMN a IN PARTITION', ' 1', ['command_list.children.0.command_type', 'command_list.children.0.clear_column', 'command_list.children.0.column']],
            ['ALTER TABLE t MOVE PARTITION 1 TO TABLE', ' t2', ['command_list.children.0.command_type', 'command_list.children.0.move_destination_type',
                'command_list.children.0.partition']],
            ['ALTER TABLE t ATTACH PARTITION 1 FROM', ' t2', ['command_list.children.0.command_type', 'command_list.children.0.replace', 'command_list.children.0.partition']],
            ['ALTER TABLE t UPDATE a = 1 WHERE', ' 1', ['command_list.children.0.command_type', 'command_list.children.0.update_assignments']],
            ['ALTER TABLE t MODIFY REFRESH EVERY 1 HOUR OFFSET', ' 5 MINUTE', ['command_list.children.0.command_type', 'command_list.children.0.refresh.period']],
            ['ALTER DATABASE d MODIFY SETTING', ' x = 1', ['database_ast', 'alter_object', 'command_list.children.0.command_type']],
            /// The `SETTINGS` of the `SELECT` of an `INSERT` are in those of the `INSERT` too.
            ['INSERT INTO t SETTINGS a = 2 SELECT 1 SETTINGS max_threads = 1, a = 3 FORMAT', ' CSV', ['settings_ast', 'select']],
            /// A `PRIMARY KEY` in the column list is in the storage definition, also one that failed.
            ['CREATE TABLE t (a UInt8, PRIMARY KEY a) ENGINE = MergeTree ORDER BY', ' a', ['columns_list', 'storage.engine',
                'storage.primary_key']],
            ['CREATE TABLE t (a UInt8 PRIMARY KEY) ENGINE = MergeTree ORDER BY', ' a', ['columns_list', 'storage.primary_key']],
            ['CREATE TABLE t (a UInt8, PRIMARY KEY a) COMMENT', " 'c'", ['columns_list', 'storage']],
            ['CREATE MATERIALIZED VIEW v (a UInt8, PRIMARY KEY a) ENGINE = MergeTree ORDER BY a', ' AS SELECT 1 AS a',
                ['columns_list', 'targets']],
            ['CREATE MATERIALIZED VIEW v (a UInt8, PRIMARY KEY a) ENGINE = MergeTree ORDER BY', ' a AS SELECT 1 AS a',
                ['columns_list', 'targets.targets.0.inner_engine.engine', 'targets.targets.0.inner_engine.primary_key']],
            /// `TO INNER UUID` is in `targets`, as in `ast`.
            ["CREATE TABLE t TO INNER UUID '123e4567-e89b-12d3-a456-426614174000' (a UInt8,", ' b UInt8) ENGINE = SharedSet',
                ['targets', 'has_inner_uuid_clause']],
            ['CREATE TABLE t AS db.', 'other', ['as_database']],
            /// A `TimeSeries` table is one from its engine on, also when its storage definition failed.
            [`${T} ENGINE = TimeSeries ORDER BY`, ' a', ['is_time_series_table', 'storage.engine']],
            ['CREATE MATERIALIZED VIEW v REFRESH RANDOMIZE FOR 1 SECOND', ' DEPENDS ON x AS SELECT 1', ['refresh_strategy.spread']],
        ]) {
            const partial = parsed(prefix).doc?.partial_ast;
            let ast = parsed(prefix + rest).doc?.ast;
            if (ast?.type === 'SelectWithUnionQuery')
                ast = ast.list_of_selects.children[0];
            check(`...with ${keys.join(', ')} as in the ast of the whole statement: ${prefix.slice(0, 40)}`,
                keys.every(k => at(partial, k) !== undefined && JSON.stringify(at(partial, k)) === JSON.stringify(at(ast, k))));
        }

        /// A `WITH` written before `INSERT` is in each `SELECT` of the union of the `INSERT`, as in the
        /// "ast" of the whole statement, and not in a `SELECT` nested in one of them.
        const outer = 'WITH c AS (SELECT 1) INSERT INTO t';
        const union = 'select.list_of_selects.children';
        for (const [prefix, rest, path, hasWith] of [
            [`${outer} SELECT * FROM c WHERE`, ' 1', `${union}.0`, true],
            [`${outer} SELECT 1 UNION ALL SELECT * FROM c WHERE`, ' 1', `${union}.1`, true],
            [`${outer} (SELECT * FROM c WHERE`, ' 1)', `${union}.0`, true],
            [`${outer} SELECT 1 UNION ALL (SELECT * FROM c WHERE`, ' 1)', `${union}.1`, true],
            [`${outer} SELECT * FROM (SELECT a FROM`, ' c)',
                `${union}.0.tables.children.0.table_expression.subquery.children.0.list_of_selects.children.0`, false],
            [`${outer} SELECT * FROM c WHERE x IN (SELECT a FROM`, ' c)',
                `${union}.0.where.arguments.children.1.children.0.list_of_selects.children.0`, false],
        ]) {
            const partial = parsed(prefix).doc?.partial_ast;
            const select = at(parsed(prefix + rest).doc?.ast, path);
            check(`...${hasWith ? 'with' : 'without'} the WITH before INSERT, as in the ast: ${prefix.slice(outer.length + 1, outer.length + 41)}`,
                partial?.type === 'SelectQuery' && select?.type === 'SelectQuery' && (partial.with !== undefined) === hasWith
                && JSON.stringify(partial.with) === JSON.stringify(select.with));
        }

        /// Past the limits of an "ast" there is no partial_ast, and the error is the one without it.
        const wide = `SELECT ${Array(60000).fill('a').join(',')} FROM`;
        const wideParsed = parsed(wide);
        check('a partial tree over the element limit is not reported', !wideParsed.ok
            && wideParsed.doc?.partial_ast === undefined && wideParsed.doc?.error?.message === format(wide, 1).out);
        const deep = `SELECT ${Array(5000).fill('1').join(' + ')} FROM`;
        const deepParsed = parsed(deep);
        check('nor one over the depth limit', !deepParsed.ok
            && deepParsed.doc?.partial_ast === undefined && deepParsed.doc?.error?.message === format(deep, 1).out);
        /// Nor the statement around it that failed at the same place.
        for (const around of [`SELECT * FROM (${deep}`, `SELECT 1 WHERE x IN (${deep}`]) {
            const r = parsed(around);
            check(`nor the query around it: ${around.slice(0, 30)}...`, !r.ok
                && r.doc?.partial_ast === undefined && r.doc?.error?.message === format(around, 1).out);
        }
        /// A document `ch_format_json` would not read is turned away as an "ast" is: one node, over 1 MiB.
        const literal = `[${Array(60000).fill('1').join(', ')}]`;
        const bigAst = parsed(`SELECT ${literal} FROM t`).doc;
        const bigPartial = parsed(`SELECT ${literal} FROM`).doc;
        check('a partial_ast over the document size of an ast is null, for the same reason', bigAst?.ast === null
            && bigPartial?.partial_ast === null && /too big/.test(bigAst?.ast_error ?? '')
            && bigPartial?.partial_ast_error === bigAst.ast_error);
    }
}

/// --- The engine's stack -----------------------------------------------------------------------
///
/// WebAssembly frames also take the engine's own stack, which `checkStackSize` cannot see, and
/// running out of it is a `RangeError` that leaves the instance unusable. That stack is smallest
/// in a Web Worker in Chrome, where both the stack size in CMakeLists.txt and
/// `MAX_AST_JSON_NESTING` in wasm_parser.cpp were measured. Node's main thread has more of it than
/// that, so the deepest inputs are driven here again in a worker whose V8 stack is limited to
/// about what a Chrome worker has: every one must come back as a result or an error. Each runs in
/// a fresh worker on a freshly compiled module - the extra custom section keeps V8 from reusing
/// code optimized by an earlier run - because a cold run takes the most stack.

const WORKER_STACK_MB = 0.75;

function inWorker(entry, text) {
    const nonce = Buffer.from(`n${Math.random()}`);
    const section = Buffer.concat([Buffer.from([1]), Buffer.from('n'), nonce]);
    const module = Buffer.concat([bytes, Buffer.from([0, section.length]), section]);
    const source = `
        const { parentPort, workerData } = require('node:worker_threads');
        const { WASI } = require('node:wasi');
        const wasi = new WASI({ version: 'preview1', args: [], env: {}, returnOnExit: true });
        const instance = new WebAssembly.Instance(new WebAssembly.Module(workerData.module), wasi.getImportObject());
        wasi.initialize(instance);
        const x = instance.exports;
        const input = new TextEncoder().encode(workerData.text);
        const ptr = x.ch_alloc(input.length);
        new Uint8Array(x.memory.buffer, ptr, input.length).set(input);
        try {
            const ok = workerData.entry === 'ch_parse' ? x.ch_parse(ptr, input.length) : x[workerData.entry](ptr, input.length, 1);
            const out = new TextDecoder().decode(new Uint8Array(x.memory.buffer, x.ch_result_data(), x.ch_result_size()).slice());
            parentPort.postMessage({ ok: !!ok, out });
        } catch (e) {
            parentPort.postMessage({ trap: e.constructor.name + ': ' + e.message });
        }`;
    return new Promise(resolve => {
        const worker = new Worker(source, { eval: true, workerData: { module, entry, text }, resourceLimits: { stackSizeMb: WORKER_STACK_MB } });
        worker.on('message', m => { resolve(m); worker.terminate(); });
        worker.on('error', e => resolve({ trap: e.message }));
    });
}

console.log(`\n--- with a ${WORKER_STACK_MB} MB engine stack ---`);
{
    /// The deepest SQL each of these shapes admits, from `MAX_PARSER_DEPTH` and the AST depth
    /// limit, and one level past it.
    const deep = [
        ['197 nested parentheses', `SELECT ${'('.repeat(197)}1${')'.repeat(197)}`],
        ['109 nested IN subqueries', `SELECT * FROM t WHERE ${'x IN (SELECT y FROM u WHERE '.repeat(109)}1${')'.repeat(109)}`],
        ['a CAST to an Array nested 984 levels', `SELECT CAST(1 AS ${'Array('.repeat(984)}UInt8${')'.repeat(984)})`],
        ['an array literal nested 983 levels', `SELECT ${'['.repeat(983)}1${']'.repeat(983)}`],
        ['a chain of 498 terms', `SELECT ${Array(498).fill('1').join(' + ')}`],
        ['a chain of 499 terms', `SELECT ${Array(499).fill('1').join(' + ')}`],
    ];
    for (const [name, sql] of deep) {
        const r = await inWorker('ch_parse', sql);
        check(`ch_parse of ${name} answers`, !r.trap);
        if (canFormat) {
            const f = await inWorker('ch_format', sql);
            check(`ch_format of ${name} answers`, !f.trap);
        }
    }

    /// The same shapes, failing after the deep part, where a statement parser captures the tree so far:
    /// a tree too deep for an "ast" is not copied, and the error is the one a parse without capture
    /// reports - `ch_format` does not capture.
    const terms = n => Array(n).fill('1').join(' + ');
    const failing = [
        ['a WHERE of 498 terms, then GROUP BY', `SELECT 1 WHERE ${terms(498)} GROUP BY`],
        ['a WHERE of 499 terms, then GROUP BY', `SELECT 1 WHERE ${terms(499)} GROUP BY`],
        ['a WHERE of 5000 terms, then GROUP BY', `SELECT 1 WHERE ${terms(5000)} GROUP BY`],
        ['a select list of 5000 terms, then FROM', `SELECT ${terms(5000)} FROM`],
        ['a subquery with a select list of 5000 terms, then FROM', `SELECT * FROM (SELECT ${terms(5000)} FROM`],
        ['5000 subscripts, then FROM', `SELECT x${'[1]'.repeat(5000)} FROM`],
        ['3000 casts with ::, then FROM', `SELECT 1${'::UInt8'.repeat(3000)} FROM`],
        ['a DEFAULT of 5000 terms, then ENGINE =', `CREATE TABLE t (a UInt8 DEFAULT ${terms(5000)}) ENGINE =`],
        ['INSERT ... SELECT with a WHERE of 3000 terms, then GROUP BY', `INSERT INTO t SELECT 1 WHERE ${terms(3000)} GROUP BY`],
        ['INSERT ... SELECT of 3000 terms, then FORMAT', `INSERT INTO t SELECT ${terms(3000)} FORMAT`],
        /// The engine of a storage definition that failed is measured before anything copies it.
        ['engine arguments of 5000 terms, then ORDER BY', `CREATE TABLE t (a UInt8) ENGINE = MergeTree(${terms(5000)}) ORDER BY`],
        ['engine arguments of 5000 terms, then SETTINGS', `CREATE TABLE t (a UInt8) ENGINE = MergeTree(${terms(5000)}) SETTINGS`],
        ['view engine arguments of 5000 terms, then ORDER BY',
            `CREATE MATERIALIZED VIEW v ENGINE = MergeTree(${terms(5000)}) ORDER BY`],
        ['database engine arguments of 5000 terms, then SETTINGS', `CREATE DATABASE d ENGINE = Replicated(${terms(5000)}) SETTINGS`],
        ['INSERT SETTINGS with an array nested 900 levels, then FORMAT after SELECT ... SETTINGS',
            `INSERT INTO t SETTINGS a = ${'['.repeat(900)}1${']'.repeat(900)} SELECT 1 SETTINGS b = 1 FORMAT`],
    ];
    for (const [name, sql] of failing) {
        const r = await inWorker('ch_parse', sql);
        if (!canFormat) {
            check(`ch_parse of ${name} answers`, !r.trap && !r.ok);
            continue;
        }
        const f = await inWorker('ch_format', sql);
        check(`ch_parse of ${name} answers with the error of a parse without capture`,
            !r.trap && !f.trap && !r.ok && JSON.parse(r.out).error?.message === f.out);
    }

    if (hasAstJson) {
        /// Documents no query produces, which `createFromJSON` alone would admit.
        for (const n of [2001, 4520, 7999]) {
            for (const [kind, json] of [
                ['arrays', `{"type":"Function","children":${'['.repeat(n)}${']'.repeat(n)}}`],
                ['objects', `{"type":"Function","children":${'{"a":'.repeat(n)}1${'}'.repeat(n)}}`],
            ]) {
                const r = await inWorker('ch_format_json', json);
                check(`ch_format_json of ${kind} nested ${n} levels is an error`,
                    !r.trap && !r.ok && /nested too deeply/.test(r.out));
            }
        }
    }
}

const notes = [canFormat ? null : 'no formatting', hasDcl ? null : 'no DCL', hasAstJson ? null : 'no AST JSON'].filter(Boolean);
console.log(`\n${pass}/${total} passed${notes.length ? ` (${notes.join(', ')})` : ''}`);
process.exit(pass === total ? 0 : 1);
