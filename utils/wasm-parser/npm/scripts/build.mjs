/// Build both wasm configurations, copy them next to this package, and pack the tarball.
/// Requires `npm run setup` (or a valid `WASI_SDK`). Artifacts are generated; they are not committed.
import { spawnSync } from 'node:child_process';
import { cpSync, existsSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defaultSdk, repoRoot, sdkIsValid, toolchainFile } from './wasi-sdk.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const npmRoot = dirname(here);
const cmakeSource = resolve(npmRoot, '..');
const outDir = join(repoRoot, 'tmp', 'wasm-parser');

// Keep in sync with SUBMODULES in ci/jobs/build_wasm_parser.py.
const SUBMODULES = [
    'contrib/abseil-cpp',
    'contrib/boost',
    'contrib/cctz',
    'contrib/croaring',
    'contrib/double-conversion',
    'contrib/fast_float',
    'contrib/fmtlib',
    'contrib/libdivide',
    'contrib/magic_enum',
    'contrib/miniselect',
    'contrib/re2',
    'contrib/sparsehash-c11',
    'contrib/wyhash',
    'contrib/xxHash',
    'contrib/zmij',
];

const CONFIGURATIONS = [
    { name: 'everything', options: [], artifact: 'parser.wasm' },
    {
        name: 'no-formatting-no-dcl',
        options: ['-DENABLE_FORMATTING=OFF', '-DENABLE_DCL=OFF'],
        artifact: 'parser-no-formatting-no-dcl.wasm',
    },
];

function fail(message)
{
    process.stderr.write(message + '\n');
    process.exit(1);
}

function resolveWasiSdk()
{
    if (process.env.WASI_SDK)
    {
        if (!sdkIsValid(process.env.WASI_SDK))
            fail(`WASI_SDK=${process.env.WASI_SDK} does not contain share/cmake/wasi-sdk-p1.cmake`);
        return process.env.WASI_SDK;
    }
    if (sdkIsValid(defaultSdk))
        return defaultSdk;
    fail('wasi-sdk is not set up. Run `npm run setup` in utils/wasm-parser/npm first.');
}

function run(command, args, options = {})
{
    const result = spawnSync(command, args, {
        encoding: 'utf8',
        stdio: 'inherit',
        ...options,
    });
    if (result.error?.code === 'ENOENT')
        fail(`${command} is not on PATH`);
    if (result.status !== 0)
        fail(`${command} ${args.join(' ')} failed (${result.status})`);
}

function submoduleEmpty(path)
{
    const abs = join(repoRoot, path);
    if (!existsSync(abs))
        return true;
    return readdirSync(abs).length === 0;
}

function ensureSubmodules()
{
    if (!SUBMODULES.some(submoduleEmpty))
        return;
    run('git', ['submodule', 'sync', '--', ...SUBMODULES], { cwd: repoRoot });
    run(
        'git',
        ['submodule', 'update', '--init', '--depth', '1', '--single-branch', '--jobs', '10', '--', ...SUBMODULES],
        { cwd: repoRoot },
    );
}

function buildConfiguration(wasiSdk, { name, options, artifact })
{
    const configDir = join(outDir, name);
    mkdirSync(configDir, { recursive: true });
    run('cmake', [
        '-S',
        cmakeSource,
        '-B',
        configDir,
        '-G',
        'Ninja',
        `-DCMAKE_TOOLCHAIN_FILE=${toolchainFile(wasiSdk)}`,
        `-DWASI_SDK_PREFIX=${wasiSdk}`,
        ...options,
    ]);
    run('cmake', ['--build', configDir]);
    run('ctest', ['--test-dir', configDir, '--output-on-failure']);
    const built = join(configDir, 'parser.wasm');
    if (!existsSync(built))
        fail(`cmake --build ${configDir} did not produce parser.wasm`);
    cpSync(built, join(npmRoot, artifact));
    process.stdout.write(`copied ${artifact}\n`);
}

const wasiSdk = resolveWasiSdk();
process.stdout.write(`WASI_SDK=${wasiSdk}\n`);
ensureSubmodules();
mkdirSync(outDir, { recursive: true });

for (const config of CONFIGURATIONS)
{
    process.stdout.write(`\n=== ${config.name} ===\n`);
    buildConfiguration(wasiSdk, config);
}

run(process.execPath, [
    join(here, 'pack.mjs'),
    '--wasm-dir',
    npmRoot,
    '--out-dir',
    outDir,
]);
