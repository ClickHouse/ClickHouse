import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
export const repoRoot = resolve(here, '../../../..');
export const defaultSdk = join(repoRoot, 'tmp', 'wasi-sdk');

export function toolchainFile(prefix)
{
    return join(prefix, 'share', 'cmake', 'wasi-sdk-p1.cmake');
}

export function sdkIsValid(prefix)
{
    return existsSync(toolchainFile(prefix));
}
