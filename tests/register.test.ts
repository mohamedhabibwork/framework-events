import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

const FIXTURE = join(import.meta.dir, 'fixtures/register');

describe('Register augmentation', () => {
  test('an app augmenting Register.events gets checked payloads and typed handlers', async () => {
    const typecheck = Bun.spawn(['bunx', 'tsc', '-p', 'tsconfig.json'], {
      cwd: FIXTURE,
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [stdout, stderr, code] = await Promise.all([
      new Response(typecheck.stdout).text(),
      new Response(typecheck.stderr).text(),
      typecheck.exited,
    ]);

    expect(`${code}: ${stdout}${stderr}`).toBe('0: ');
  }, 120_000);
});
