// Validate structure only. Steam IDs are intentionally never inspected.
export function validateTyoText(text) {
  let data;
  try {
    data = JSON.parse(text.replace(/^\uFEFF/, ''));
  } catch {
    return { ok: false, code: 'not_json', message: 'This file is not valid JSON. Pick the Top->Out results JSON file for your event.' };
  }
  const fail = message => ({ ok: false, code: 'not_tyo', message });
  const settings = data && data.settings;
  if (settings && Object.hasOwn(settings, 'threshold')) {
    return fail("This looks like a Kerki/topout results file, not Tag You're Out. Pick the Tag You're Out results JSON file.");
  }
  if (!settings || !Number.isInteger(settings.lives)) {
    return fail("Tag You're Out results must have settings.lives as a whole number.");
  }
  if (!Array.isArray(data.rounds) || data.rounds.length === 0) {
    return fail("Tag You're Out results must contain a non-empty rounds array.");
  }
  for (const [i, round] of data.rounds.entries()) {
    if (!round || !round.level || typeof round.level.Name !== 'string' || !round.level.Name.trim()) {
      return fail('Round ' + (i + 1) + ' is missing its level.Name. Pick a complete results JSON file.');
    }
    if (!Array.isArray(round.playerResults) || round.playerResults.length === 0) {
      return fail('Round ' + (i + 1) + ' must contain a non-empty playerResults array.');
    }
  }
  return { ok: true, nRounds: data.rounds.length };
}
