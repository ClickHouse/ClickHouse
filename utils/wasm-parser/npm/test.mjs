/// Tests for `@clickhouse/wasm-parser`. The public seam is `Parser`: init, parse, format,
/// formatJson, features. Without `--wasm-dir`, only the pre-init contract is checked, so this
/// file can run from a checkout that has no module. With `--wasm-dir` pointing at the two
/// artifacts `Build (wasm_parser)` publishes, the C ABI is driven through the wrapper.
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Parser } from './src/full-node.mjs';
import { Parser as SlimParser } from './src/slim-node.mjs';
import { createParser } from './src/parser.mjs';
import { instantiate } from './src/wasi-node.mjs';
import { instantiate as instantiateBrowser } from './src/wasi-browser.mjs';

const here = dirname(fileURLToPath(import.meta.url));

function optionalArg(name)
{
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : undefined;
}

const wasmDir = optionalArg('--wasm-dir');

let pass = 0;
let total = 0;

function check(name, condition)
{
    total++;
    pass += condition ? 1 : 0;
    console.log(`${condition ? 'ok  ' : 'FAIL'} ${name}`);
}

function throwsInitHint(fn)
{
    try
    {
        fn();
        return false;
    }
    catch (error)
    {
        return error instanceof TypeError && /await Parser\.init\(\) first/.test(error.message);
    }
}

check('parse before init throws', throwsInitHint(() => Parser.parse('SELECT 1')));
check('format before init throws', throwsInitHint(() => Parser.format('SELECT 1')));
check('formatJson before init throws', throwsInitHint(() => Parser.formatJson('{}')));
check('features before init throws', throwsInitHint(() => Parser.features));
check('slim parse before init throws', throwsInitHint(() => SlimParser.parse('SELECT 1')));

{
    const previousFetch = globalThis.fetch;
    const fetched = [];
    globalThis.fetch = async (href) =>
    {
        fetched.push(href);
        return {
            ok: true,
            status: 200,
            arrayBuffer: async () => new Uint8Array([1, 2, 3]).buffer,
        };
    };
    try
    {
        const stub = createParser({
            instantiate: async () => ({ exports: {} }),
            wasmURL: new URL('file:///unused.wasm'),
        });
        await stub.init({ url: '/assets/parser.wasm' });
        check(
            'init({ url: /assets/parser.wasm }) fetches',
            fetched.length === 1 && fetched[0] === '/assets/parser.wasm');
    }
    finally
    {
        globalThis.fetch = previousFetch;
    }
}

{
    const tmpRoot = join(here, '../../../tmp');
    await mkdir(tmpRoot, { recursive: true });
    const packDir = await mkdtemp(join(tmpRoot, 'wasm-parser-pack-'));
    const wasmStubDir = join(packDir, 'wasm');
    const outDir = join(packDir, 'out');
    await mkdir(wasmStubDir);
    await mkdir(outDir);
    await writeFile(join(wasmStubDir, 'parser.wasm'), 'wasm-stub');
    await writeFile(join(wasmStubDir, 'parser-no-formatting-no-dcl.wasm'), 'wasm-stub-slim');

    const packed = spawnSync(
        process.execPath,
        [
            join(here, 'scripts/pack.mjs'),
            '--wasm-dir', wasmStubDir,
            '--out-dir', outDir,
            '--source-dir', here,
            '--version', '26.9.1-dev.testpack',
        ],
        { encoding: 'utf8' },
    );
    check('pack.mjs exits 0', packed.status === 0);
    if (packed.status !== 0)
        console.log(packed.stdout + packed.stderr);

    const tarball = join(outDir, 'clickhouse-wasm-parser-26.9.1-dev.testpack.tgz');
    let listed = '';
    if (packed.status === 0)
    {
        const tar = spawnSync('tar', ['-tzf', tarball], { encoding: 'utf8' });
        listed = tar.stdout;
        check('pack writes the tarball', tar.status === 0 && listed.length > 0);
    }
    else
        check('pack writes the tarball', false);

    check('tarball contains parser.wasm', listed.includes('package/parser.wasm'));
    check(
        'tarball contains slim wasm',
        listed.includes('package/parser-no-formatting-no-dcl.wasm'));
    check('tarball contains the ESM wrapper', listed.includes('package/src/parser.mjs'));
    check('tarball does not contain pack.mjs', !listed.includes('package/scripts/pack.mjs'));
    check('tarball does not contain tests', !listed.includes('package/test.mjs'));

    const stable = spawnSync('tar', ['-tzf', join(outDir, 'clickhouse-wasm-parser.tgz')], {
        encoding: 'utf8',
    });
    check(
        'pack writes clickhouse-wasm-parser.tgz',
        stable.status === 0 && stable.stdout.includes('package/parser.wasm'));

    const publishedPkgJson = spawnSync('tar', ['-xOf', tarball, 'package/package.json'], {
        encoding: 'utf8',
    });
    const publishedPkg = publishedPkgJson.status === 0 ? JSON.parse(publishedPkgJson.stdout) : null;
    check(
        'checkout pack strips scripts',
        publishedPkg !== null && publishedPkg.scripts === undefined);

    const fixtureSource = join(packDir, 'with-scripts');
    await mkdir(join(fixtureSource, 'src'), { recursive: true });
    const fixturePkg = JSON.parse(await readFile(join(here, 'package.json'), 'utf8'));
    fixturePkg.scripts = {
        setup: 'node scripts/setup.mjs',
        build: 'node scripts/build.mjs',
        test: 'node test.mjs --wasm-dir .',
    };
    await writeFile(join(fixtureSource, 'package.json'), JSON.stringify(fixturePkg, null, 2) + '\n');
    await writeFile(join(fixtureSource, 'README.md'), '# fixture\n');
    await writeFile(join(fixtureSource, 'src/parser.mjs'), '');
    const stripOut = join(packDir, 'strip-out');
    await mkdir(stripOut);
    const stripped = spawnSync(
        process.execPath,
        [
            join(here, 'scripts/pack.mjs'),
            '--wasm-dir', wasmStubDir,
            '--out-dir', stripOut,
            '--source-dir', fixtureSource,
            '--version', '26.9.1-dev.noscripts',
        ],
        { encoding: 'utf8' },
    );
    check('pack with scripts in source exits 0', stripped.status === 0);
    if (stripped.status !== 0)
        console.log(stripped.stdout + stripped.stderr);
    const packedJson = spawnSync(
        'tar',
        ['-xOf', join(stripOut, 'clickhouse-wasm-parser-26.9.1-dev.noscripts.tgz'), 'package/package.json'],
        { encoding: 'utf8' },
    );
    const parsedPkg = packedJson.status === 0 ? JSON.parse(packedJson.stdout) : null;
    check(
        'packed package.json has no scripts',
        packedJson.status === 0 && parsedPkg !== null && parsedPkg.scripts === undefined);
    check(
        'tarball does not contain setup.mjs',
        !listed.includes('package/scripts/setup.mjs'));

    const treeVersionOut = join(packDir, 'from-tree');
    await mkdir(treeVersionOut);
    const packedFromTree = spawnSync(
        process.execPath,
        [
            join(here, 'scripts/pack.mjs'),
            '--wasm-dir', wasmStubDir,
            '--out-dir', treeVersionOut,
            '--source-dir', here,
        ],
        { encoding: 'utf8' },
    );
    check('pack without --version exits 0', packedFromTree.status === 0);
    if (packedFromTree.status !== 0)
        console.log(packedFromTree.stdout + packedFromTree.stderr);

    const versionsText = await readFile(join(here, '../../../cmake/autogenerated_versions.txt'), 'utf8');
    const major = versionsText.match(/SET\(VERSION_MAJOR (\d+)\)/)[1];
    const minor = versionsText.match(/SET\(VERSION_MINOR (\d+)\)/)[1];
    const patch = versionsText.match(/SET\(VERSION_PATCH (\d+)\)/)[1];
    const sha = spawnSync(
        'git',
        ['-c', 'safe.directory=*', '-C', join(here, '../../..'), 'rev-parse', '--short=7', 'HEAD'],
        { encoding: 'utf8' },
    );
    const expectedVersion = `${major}.${minor}.${patch}-dev.${sha.stdout.trim()}`;
    const fromTreeJson = spawnSync(
        'tar',
        ['-xOf', join(treeVersionOut, `clickhouse-wasm-parser-${expectedVersion}.tgz`), 'package/package.json'],
        { encoding: 'utf8' },
    );
    const fromTreePkg = fromTreeJson.status === 0 ? JSON.parse(fromTreeJson.stdout) : null;
    check(
        'pack without --version uses tree version',
        fromTreePkg !== null && fromTreePkg.version === expectedVersion);

    await rm(packDir, { recursive: true, force: true });
}

if (!wasmDir)
{
    console.log(`\n${pass}/${total} passed (no --wasm-dir; C ABI tests skipped)`);
    process.exit(pass === total ? 0 : 1);
}

if (!existsSync(join(wasmDir, 'parser.wasm'))
    || !existsSync(join(wasmDir, 'parser-no-formatting-no-dcl.wasm')))
{
    console.error('missing parser.wasm / parser-no-formatting-no-dcl.wasm; run `npm run build` first');
    process.exit(1);
}

const fullWasm = pathToFileURL(join(wasmDir, 'parser.wasm'));
const slimWasm = pathToFileURL(join(wasmDir, 'parser-no-formatting-no-dcl.wasm'));

await Parser.init({ url: fullWasm });
await Parser.init({ url: fullWasm });
check('init is idempotent', typeof Parser.features === 'object' && Parser.features !== null);
check('full build has formatting', Parser.features.format === true);
check('full build has AST JSON', Parser.features.astJson === true);
check('full build has DCL', Parser.features.dcl === true);

{
    const result = Parser.parse('SELECT 1');
    check('parse SELECT 1 has no error', result.error === undefined);
    check('parse SELECT 1 has an ast type', typeof result.ast?.type === 'string');
    check(
        'parse SELECT 1 highlights SELECT',
        !!result.highlights?.some(h => h.begin === 0 && h.end === 6 && h.type === 'keyword'));
}

{
    const result = Parser.parse('SELECT 1 +');
    check('parse SELECT 1 + reports an error', typeof result.error?.message === 'string');
    check('parse SELECT 1 + does not throw', result.ast === undefined);
    check('parse error lists expected tokens', Array.isArray(result.error?.expected));
}

{
    const result = Parser.format('select 1', { oneLine: true });
    check('format SELECT 1 returns sql', typeof result.sql === 'string' && /SELECT/i.test(result.sql));
    check('format SELECT 1 has no error', result.error === undefined);
}

{
    const parsed = Parser.parse('SELECT 1');
    const result = Parser.formatJson(parsed.ast, { oneLine: true });
    check('formatJson round-trips SELECT 1', typeof result.sql === 'string' && result.error === undefined);
}

{
    const result = Parser.formatJson('this is not JSON', { oneLine: true });
    check('formatJson malformed JSON is an error', typeof result.error?.message === 'string');
}

await SlimParser.init({ url: slimWasm });
check('slim build has no formatting', SlimParser.features.format === false);
check('slim build has no AST JSON', SlimParser.features.astJson === false);

{
    const result = SlimParser.parse('SELECT 1');
    check('slim parse SELECT 1 has no error', result.error === undefined);
    check('slim parse has no ast', result.ast === undefined);
}

{
    const result = SlimParser.format('SELECT 1');
    check(
        'slim format reports missing build support',
        result.sql === undefined && result.error?.message === 'format is not in this build');
}

{
    const result = SlimParser.formatJson({});
    check(
        'slim formatJson reports missing build support',
        result.sql === undefined && result.error?.message === 'format is not in this build');
}

{
    const bytes = await readFile(join(wasmDir, 'parser.wasm'));
    const fromBytes = createParser({ instantiate, wasmURL: fullWasm });
    await fromBytes.init({ bytes });
    check('init({ bytes }) parses SELECT 1', fromBytes.parse('SELECT 1').error === undefined);
}

{
    const browserParser = createParser({ instantiate: instantiateBrowser, wasmURL: fullWasm });
    await browserParser.init({ url: fullWasm });
    check(
        'wasi-browser parses SELECT 1',
        browserParser.parse('SELECT 1').error === undefined);
}

console.log(`\n${pass}/${total} passed`);
process.exit(pass === total ? 0 : 1);
