/// Download wasi-sdk 33 for this OS/arch into `<repo>/tmp/wasi-sdk`, and check host tools.
/// Honors `WASI_SDK` when it already points at a usable prefix. Does not install cmake/ninja.
import { spawnSync } from 'node:child_process';
import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { defaultSdk, repoRoot, sdkIsValid, toolchainFile } from './wasi-sdk.mjs';

const WASI_SDK_VERSION = 33;

function fail(message)
{
    process.stderr.write(message + '\n');
    process.exit(1);
}

function toolHint()
{
    if (process.platform === 'darwin')
        return 'Install cmake and ninja with: brew install cmake ninja';
    if (process.platform === 'linux')
        return 'Install cmake and ninja with: apt install cmake ninja-build';
    return 'Install cmake and ninja, then retry.';
}

function requireTool(name, hint = toolHint())
{
    const result = spawnSync(name, ['--version'], { encoding: 'utf8' });
    if (result.error?.code === 'ENOENT' || result.status !== 0)
        fail(`${name} is not on PATH. ${hint}`);
}

function requireCMake()
{
    requireTool('cmake');
    const result = spawnSync('cmake', ['--version'], { encoding: 'utf8' });
    const match = result.stdout.match(/cmake version (\d+)\.(\d+)/i);
    if (!match)
        fail('could not parse `cmake --version`');
    const major = Number(match[1]);
    const minor = Number(match[2]);
    if (major < 3 || (major === 3 && minor < 24))
        fail(`cmake >= 3.24 is required (have ${match[1]}.${match[2]}). ${toolHint()}`);
}

function requireNode()
{
    const major = Number(process.versions.node.split('.')[0]);
    if (major < 22)
        fail(`Node.js >= 22 is required (have ${process.versions.node})`);
}

function wasiReleaseTriple()
{
    const { platform, arch } = process;
    if (platform === 'linux')
    {
        if (arch === 'x64')
            return 'x86_64-linux';
        if (arch === 'arm64')
            return 'arm64-linux';
    }
    if (platform === 'darwin')
    {
        if (arch === 'x64')
            return 'x86_64-macos';
        if (arch === 'arm64')
            return 'arm64-macos';
    }
    fail(`no wasi-sdk release for ${platform}/${arch} (need Linux or macOS, x64 or arm64)`);
}

function downloadSdk(dest)
{
    const triple = wasiReleaseTriple();
    const url = `https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/wasi-sdk-${WASI_SDK_VERSION}.0-${triple}.tar.gz`;
    process.stdout.write(`downloading ${url}\n`);
    const result = spawnSync(
        'sh',
        ['-c', 'curl -fsSL "$1" | tar xz -C "$2" --strip-components=1', 'setup', url, dest],
        { stdio: 'inherit' },
    );
    if (result.status !== 0)
        fail(`failed to download or unpack wasi-sdk (${result.status})`);
}

requireNode();
requireCMake();
requireTool('ninja');
requireTool('git', 'Install git, then retry.');
requireTool('curl', 'Install curl, then retry.');

if (process.env.WASI_SDK)
{
    if (!sdkIsValid(process.env.WASI_SDK))
        fail(`WASI_SDK=${process.env.WASI_SDK} does not contain share/cmake/wasi-sdk-p1.cmake`);
    process.stdout.write(`using WASI_SDK=${process.env.WASI_SDK}\n`);
    process.exit(0);
}

if (sdkIsValid(defaultSdk))
{
    process.stdout.write(`using existing ${defaultSdk}\n`);
    process.exit(0);
}

await rm(defaultSdk, { recursive: true, force: true });
await mkdir(defaultSdk, { recursive: true });
downloadSdk(defaultSdk);
if (!sdkIsValid(defaultSdk))
{
    await rm(defaultSdk, { recursive: true, force: true });
    fail(`unpacked wasi-sdk at ${defaultSdk} is missing ${toolchainFile(defaultSdk)}`);
}
process.stdout.write(`WASI_SDK=${defaultSdk}\n`);
