/// Drive the standalone WebAssembly SQL parser and check that it parses, formats, and reports
/// structured results (`ch_parse` / `ch_format_json`).
import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';

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
    check('ch_parse works after ch_format_json errors', parsed('SELECT 1 + 2').ok);

    /// Multi-line formatting through the JSON path matches the direct path.
    const sql = cases[1];
    const viaJson = call(JSON.stringify(parsed(sql).doc.ast), (ptr, len) => ch_format_json(ptr, len, 0));
    check('ch_format_json multi-line matches ch_format', viaJson.ok && viaJson.out === format(sql, 0).out);
}

const notes = [canFormat ? null : 'no formatting', hasDcl ? null : 'no DCL', hasAstJson ? null : 'no AST JSON'].filter(Boolean);
console.log(`\n${pass}/${total} passed${notes.length ? ` (${notes.join(', ')})` : ''}`);
process.exit(pass === total ? 0 : 1);
