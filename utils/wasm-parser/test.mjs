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
