import test from 'node:test';
import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { validateTyoText } from '../src/validate.js';

const logs = new URL('../../logs/', import.meta.url);
const names = (await readdir(logs)).filter(name => /^TYO_.*\.json$/.test(name));
assert.ok(names.length > 0, 'Repository TyO logs must be present');
for (const name of names) {
  test('accepts repository log ' + name, async () => {
    // readFile preserves the BOM; pass the original text to the validator.
    const text = await readFile(new URL(name, logs), 'utf8');
    const parsed = JSON.parse(text.replace(/^\uFEFF/, ''));
    assert.deepEqual(validateTyoText(text), { ok: true, nRounds: parsed.rounds.length });
  });
}
const valid = () => ({ settings: { lives: 3 }, rounds: [{ level: { Name: 'Track' }, playerResults: [{}] }] });
const rejects = (value, code = 'not_tyo') => {
  const result = validateTyoText(JSON.stringify(value));
  assert.equal(result.ok, false);
  assert.equal(result.code, code);
  assert.ok(result.message.length > 0);
};
test('rejects invalid JSON', () => {
  assert.equal(validateTyoText('{broken').code, 'not_json');
});
test('rejects Kerki/topout settings with a friendly explanation', () => {
  const value = valid();
  value.settings = { threshold: 25, winners: 3 };
  rejects(value);
  assert.match(validateTyoText(JSON.stringify(value)).message, /Kerki\/topout.*not Tag You're Out/);
  value.settings.lives = 3;
  rejects(value);
});
test('rejects lives as a string', () => { const v = valid(); v.settings.lives = '3'; rejects(v); });
test('rejects empty rounds', () => { const v = valid(); v.rounds = []; rejects(v); });
test('rejects a round without level.Name', () => { const v = valid(); delete v.rounds[0].level.Name; rejects(v); });
test('rejects a round with empty playerResults', () => { const v = valid(); v.rounds[0].playerResults = []; rejects(v); });
test('accepts a leading BOM', () => {
  assert.deepEqual(validateTyoText('\uFEFF' + JSON.stringify(valid())), { ok: true, nRounds: 1 });
});
