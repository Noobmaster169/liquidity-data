import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadRegistry } from '../src/registry.ts';

test('registry has ten tier 1 programs, prop AMMs in tier 2, routers separate, exclusions visible', () => {
  const r = loadRegistry('config/venues.json', 'config/routers.json');
  assert.equal(r.tier1.size, 10);
  assert.equal(r.tier1.get('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo')?.label, 'meteora_dlmm');
  assert.equal(r.tier2.get('TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH')?.kind, 'prop');
  assert.equal(r.tier2.get('BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi')?.label, 'bisonfi');
  assert.ok(r.isVenue('9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'));
  assert.ok(!r.isVenue('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'));
  assert.equal(r.routers.get('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'), 'jupiter_v6');
  assert.equal(r.labelOf('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'), 'jupiter_v6');
  assert.ok(!r.isVenue('stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq'), 'Sanctum router is excluded');
});
