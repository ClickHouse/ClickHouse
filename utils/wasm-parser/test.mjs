/// Drive the standalone WebAssembly SQL parser and check that it parses and formats.
import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';

const wasi = new WASI({ version: 'preview1', args: [], env: {}, returnOnExit: true });
const bytes = await readFile(process.argv[2] ?? 'tmp/wasmexp/parser_stripped.wasm');
const { instance } = await WebAssembly.instantiate(bytes, wasi.getImportObject());
wasi.initialize(instance);

const { memory, ch_features, ch_alloc, ch_free, ch_check, ch_format, ch_result_data, ch_result_size } = instance.exports;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const FEATURE_FORMAT = 1, FEATURE_DCL = 2;
const features = ch_features();
const canFormat = !!(features & FEATURE_FORMAT);
const hasDcl = !!(features & FEATURE_DCL);

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
    if (!canFormat)
        return call(sql, ch_check);
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

let pass = 0;
for (const sql of cases) {
    const r = format(sql, 1);
    const expectFail = expectedToFail.has(sql) || (!hasDcl && dclCases.includes(sql));
    const good = expectFail ? !r.ok : r.ok;
    pass += good ? 1 : 0;
    console.log(`${good ? 'ok  ' : 'FAIL'} ${r.ok ? '' : '[error] '}${r.out.replace(/\n/g, ' ').slice(0, 110)}`);
}
if (canFormat)
    console.log(`\n--- multi-line formatting ---\n${format(cases[1], 0).out}`);
const notes = [canFormat ? null : 'no formatting', hasDcl ? null : 'no DCL'].filter(Boolean);
console.log(`\n${pass}/${cases.length} passed${notes.length ? ` (${notes.join(', ')})` : ''}`);
process.exit(pass === cases.length ? 0 : 1);
