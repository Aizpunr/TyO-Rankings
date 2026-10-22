import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(import.meta.url);
const preview = require('../../tyo_preview.js');
const root = fileURLToPath(new URL('../../', import.meta.url));
const expected = JSON.parse(readFileSync(path.join(root, 'tests/fixtures/preview_expected.json'), 'utf8'));
let checked = 0;
let matched = 0;
let failed = false;
const checkedEvents = new Set();
const files = readdirSync(path.join(root, 'logs')).filter(name => /^TYO_\d+_\d{8}\.json$/.test(name));
files.sort((a, b) => Number(a.split('_')[1]) - Number(b.split('_')[1]));
for (const file of files) {
  const event = String(Number(file.split('_')[1]));
  if (!Object.hasOwn(expected, event)) continue;
  checked += 1;
  checkedEvents.add(event);
  try {
    const data = preview.parseTyoText(readFileSync(path.join(root, 'logs', file), 'utf8'));
    assert.deepEqual(preview.validateShape(data), [], 'shape validation');
    const result = preview.previewCup(data);
    const actual = { winner_steamid: result.winnerSteamid, lobby_size: result.lobbySize,
      rounds_total: result.roundsTotal, format: result.format, placements: result.placements };
    for (const field of Object.keys(actual)) {
      assert.deepEqual(actual[field], expected[event][field], `cup ${event} field ${field}`);
    }
    matched += 1;
    console.log(`ok ${event}`);
  } catch (error) {
    failed = true;
    console.error(`mismatch ${event}: ${error.message}`);
  }
}

function rule(name, check) {
  try { check(); }
  catch (error) {
    failed = true;
    console.error(`rule ${name}: ${error.message}`);
  }
}
rule('steamID precision', () => {
  const data = preview.parseTyoText('\uFEFF{"steamID":76561198315900248,"targetedBySteamID":76561198315900248,"targetSteamID":76561198315900248}');
  for (const key of Object.keys(data)) assert.equal(data[key], '76561198315900248');
});
rule('Kerki rejection', () => {
  assert.ok(preview.validateShape({ settings: { threshold: 25, winners: 3 } })
    .includes("this looks like a Kerki/topout file, not Tag You're Out"));
});
rule('empty rounds rejection', () => {
  assert.ok(preview.validateShape({ settings: { lives: 3 }, rounds: [] }).length > 0);
});
rule('unfinished cup survivors', () => {
  const data = preview.parseTyoText(readFileSync(path.join(root, 'logs/TYO_57_20261010.json'), 'utf8'));
  data.rounds = data.rounds.slice(0, 6);
  assert.ok(preview.previewCup(data).survivors > 1);
});
for (const event of Object.keys(expected)) {
  if (!checkedEvents.has(event)) {
    failed = true;
    console.error(`missing log for cup ${event}`);
  }
}
if (checkedEvents.size < 34) {
  failed = true;
  console.error(`only ${checkedEvents.size} distinct cups checked; expected at least 34`);
}
console.log(`${matched}/${checked} cups match`);
if (failed) process.exitCode = 1;
