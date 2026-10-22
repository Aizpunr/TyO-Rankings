/* tyo_preview.js: browser and Node cup preview port of build_tyo.py.
 *
 * Python is the source of truth; fixture checks cover every logged cup.
 * No dependencies or build step. SteamIDs exceed JavaScript's safe integer
 * range, so bare ID values are quoted before JSON parsing to preserve them.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.TyoPreview = factory();
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  function has(obj, key) {
    return Object.prototype.hasOwnProperty.call(obj, key);
  }

  function parseTyoText(text) {
    try {
      return JSON.parse(text.replace(/^\uFEFF/, '').replace(
        /"(steamID|targetedBySteamID|targetSteamID)"\s*:\s*(-?\d+)/g,
        '"$1":"$2"'
      ));
    } catch (error) {
      throw new Error('Cannot parse TyO JSON: ' + error.message);
    }
  }

  function validateShape(data) {
    var errors = [];
    var settings = (data && data.settings) || {};
    var rounds = data && data.rounds;
    if (has(settings, 'threshold')) {
      errors.push("this looks like a Kerki/topout file, not Tag You're Out");
    }
    if (typeof settings.lives !== 'number' || !isFinite(settings.lives) ||
        Math.floor(settings.lives) !== settings.lives) {
      errors.push('settings.lives missing or not an integer');
    }
    if (!Array.isArray(rounds) || !rounds.length) {
      errors.push('rounds[] missing or empty');
      return errors;
    }
    rounds.forEach(function (rnd, i) {
      if (!rnd || !rnd.level || !rnd.level.Name) {
        errors.push('round ' + i + ': level.Name missing');
      }
      if (!rnd || !Array.isArray(rnd.playerResults) || !rnd.playerResults.length) {
        errors.push('round ' + i + ': playerResults missing or empty');
      }
    });
    return errors;
  }

  function deriveCup(data) {
    var livesStart = has(data.settings, 'lives') ? data.settings.lives : 3;
    var state = Object.create(null);
    var lobbySids = [];
    var credits = [];
    var roundsPlayed = Object.create(null);
    var observedNames = Object.create(null);
    Object.defineProperty(state, '_lobbySids', { value: lobbySids });
    data.rounds.forEach(function (rnd, roundIdx) {
      var results = Object.create(null);
      var roundSids = [];
      rnd.playerResults.forEach(function (pr) {
        var sid = String(pr.steamID);
        if (!has(results, sid)) roundSids.push(sid);
        results[sid] = pr;
      });
      roundSids.forEach(function (sid) {
        var pr = results[sid];
        var curLives = has(pr, 'livesRemaining') ? pr.livesRemaining : livesStart;
        var losses, pursuer, i, blow;
        if (!has(state, sid)) {
          lobbySids.push(sid);
          state[sid] = { lives: livesStart, eliminated: false, elim_round: null };
          observedNames[sid] = [];
          roundsPlayed[sid] = 0;
        }
        observedNames[sid].push(pr.username || '');
        if (state[sid].eliminated) return;
        roundsPlayed[sid] += 1;
        losses = Math.max(0, state[sid].lives - curLives);
        pursuer = pr.targetedBySteamID == null ? null : String(pr.targetedBySteamID);
        for (i = 0; i < losses; i += 1) {
          state[sid].lives -= 1;
          blow = state[sid].lives === 0;
          if (blow) {
            state[sid].eliminated = true;
            state[sid].elim_round = roundIdx;
          }
          credits.push([roundIdx, pursuer, sid, blow]);
        }
        if (pr.eliminated && !state[sid].eliminated) {
          state[sid].eliminated = true;
          state[sid].elim_round = roundIdx;
        }
      });
    });
    return { state: state, credits: credits, roundsPlayed: roundsPlayed,
      observedNames: observedNames };
  }

  function assignPlacements(state, lobbySids) {
    var placements = {};
    var byRound = Object.create(null);
    var nextRank = 1;
    lobbySids.forEach(function (sid) {
      if (!state[sid].eliminated) {
        placements[sid] = 1;
        nextRank += 1;
      } else {
        var r = state[sid].elim_round;
        if (!has(byRound, r)) byRound[r] = [];
        byRound[r].push(sid);
      }
    });
    Object.keys(byRound).sort(function (a, b) { return b - a; }).forEach(function (r) {
      byRound[r].forEach(function (sid) { placements[sid] = nextRank; });
      nextRank += byRound[r].length;
    });
    return placements;
  }

  function formatFlag(roundsTotal, uniqueMaps) {
    if (uniqueMaps === roundsTotal) return 'roulette';
    if (uniqueMaps !== 4) return 'non_standard';
    return 'standard';
  }

  function previewCup(data) {
    var derived = deriveCup(data);
    var state = derived.state;
    var lobbySids = state._lobbySids;
    var placements = assignPlacements(state, lobbySids);
    var winner = null;
    var survivors = 0;
    var maps = [];
    var uids = [];
    var names = Object.create(null);
    data.rounds.forEach(function (rnd) {
      var level = rnd.level || {};
      var uid = has(level, 'UID') ? level.UID : '';
      if (uids.indexOf(uid) === -1) uids.push(uid);
      if (maps.indexOf(level.Name || '') === -1) maps.push(level.Name || '');
    });
    var leaderboard = lobbySids.map(function (sid) {
      names[sid] = '';
      derived.observedNames[sid].forEach(function (name) {
        if (name) names[sid] = name;
      });
      if (!state[sid].eliminated) {
        survivors += 1;
        if (winner === null || state[sid].lives > state[winner].lives) winner = sid;
      }
      return { steamid: sid, name: names[sid], placement: placements[sid],
        livesLeft: state[sid].lives,
        elimRound: state[sid].elim_round === null ? null : state[sid].elim_round + 1 };
    });
    leaderboard.sort(function (a, b) {
      var an = a.name.toLowerCase();
      var bn = b.name.toLowerCase();
      return a.placement - b.placement || (an < bn ? -1 : an > bn ? 1 :
        lobbySids.indexOf(a.steamid) - lobbySids.indexOf(b.steamid));
    });
    return { winnerSteamid: winner, winnerName: winner === null ? null : names[winner],
      survivors: survivors, lobbySize: lobbySids.length, roundsTotal: data.rounds.length,
      maps: maps, format: formatFlag(data.rounds.length, uids.length),
      placements: placements, leaderboard: leaderboard };
  }

  return {
    parseTyoText: parseTyoText,
    validateShape: validateShape,
    deriveCup: deriveCup,
    assignPlacements: assignPlacements,
    formatFlag: formatFlag,
    previewCup: previewCup
  };
});
