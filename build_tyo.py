"""Build tyo.json from logs/*.json + events.md.

Reads agix's Top->Out JSON files for the Tag You're Out tournament, derives
per-cup placements + tag credits, builds per-player rollups with full tag
interaction matrix, computes an ATP-style cross-cup ranking with rolling
window + best-of drops, and writes the consolidated tyo.json.

Run: `python build_tyo.py`
"""
import json
import os
import re
import sys
import io
import glob
import datetime as dt
from collections import defaultdict, Counter
from math import inf

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from tyo_aliases import resolve, NAME_MAP

_dir = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(_dir, 'logs')
EVENTS_MD = os.path.join(_dir, 'events.md')
OUT_JSON = os.path.join(_dir, 'tyo.json')

# Ranking parameters — pure placement-based (no win bonus, no tag/elim contribution).
# Curve flattens after 2nd place; positions 21+ score 0.
PLACEMENT_PTS = [300, 200, 150, 120, 100, 85, 72, 62, 53, 45,
                  38,  32,  27,  22,  18, 14, 11,  8,  6,  4]  # idx 0 = 1st place
RANK_WINDOW = 14
RANK_BEST_OF = 10

# ELO ranking parameters (v2 — pair-based, two variants: standard + lobby-weighted)
ELO_STARTING        = 1500
ELO_K_PAIR          = 8
ELO_PROV_CUPS       = 6        # provisional 1.5x for first 6 cups attended
ELO_PROV_MULT       = 1.5
ELO_MIN_CUPS_DISPLAY = 2       # filter one-cup walk-ins from public ranking
# No decay — TyO is small, double-penalty (zero points + decay) is harsh


# ── Events.md parsing ────────────────────────────────────────────────────

def parse_events_md(path):
    """Parse the markdown table into {event_num: {date, mapper, winner}}."""
    if not os.path.exists(path):
        return {}
    out = {}
    row_re = re.compile(
        r'^\|\s*(\d+)\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*$'
    )
    with open(path, encoding='utf-8') as fp:
        for line in fp:
            m = row_re.match(line)
            if m:
                out[int(m.group(1))] = {
                    'date': m.group(2),
                    'mapper': m.group(3).strip(),
                    'winner': m.group(4).strip(),
                }
    return out


# ── Log loading ──────────────────────────────────────────────────────────

LOG_RE = re.compile(r'TYO_(\d+)_(\d{8})\.json$')


def load_logs(logs_dir):
    """Yield (event_num:int, date:str YYYY-MM-DD, data:dict, path:str) sorted."""
    entries = []
    for path in glob.glob(os.path.join(logs_dir, '*.json')):
        m = LOG_RE.search(os.path.basename(path))
        if not m:
            continue
        event = int(m.group(1))
        d = m.group(2)
        date = f'{d[:4]}-{d[4:6]}-{d[6:]}'
        with open(path, encoding='utf-8-sig') as fp:
            data = json.load(fp)
        entries.append((event, date, data, path))
    entries.sort(key=lambda e: e[0])
    return entries


# ── Tag-derivation ───────────────────────────────────────────────────────

def derive_cup(data):
    """Walk rounds; trust mod's `livesRemaining` for state, attribute each
    life loss to that round's pursuer (`targetedBySteamID`).

    The pursuer-strict-faster rule explains ~99% of life losses but the mod
    has cycle-topology edge cases (especially DNF-vs-DNF) we don't fully
    reverse-engineer. Trusting `livesRemaining` keeps state correct in all
    cases; pursuer attribution is the natural credit assignment.

    Returns (state, credits, last_seen_lives, rounds_played, observed_names_by_sid)
    """
    rounds = data['rounds']
    lives_start = data['settings'].get('lives', 3)
    state = {}                      # sid -> {lives, eliminated, elim_round}
    credits = []                    # (round_idx, tagger_sid, victim_sid, was_blow)
    last_seen_lives = {}
    rounds_played = defaultdict(int)
    observed_names = defaultdict(list)

    for round_idx, rnd in enumerate(rounds):
        results = {pr['steamID']: pr for pr in rnd['playerResults']}
        for sid, pr in results.items():
            observed_names[sid].append(pr.get('username', '') or '')

            cur_lives = pr.get('livesRemaining', lives_start)
            mod_eliminated = pr.get('eliminated', False)

            if sid not in state:
                state[sid] = {
                    'lives': lives_start,
                    'eliminated': False,
                    'elim_round': None,
                }
            prior_lives = state[sid]['lives']

            # Skip per-round counters once already eliminated; the player still
            # appears in playerResults as frozen state (lives=0, eliminated=true).
            if state[sid]['eliminated']:
                last_seen_lives[sid] = cur_lives
                continue

            rounds_played[sid] += 1
            last_seen_lives[sid] = cur_lives

            # Did the mod credit a life loss this round?
            losses = max(0, prior_lives - cur_lives)
            pursuer_sid = pr.get('targetedBySteamID')
            for _ in range(losses):
                state[sid]['lives'] -= 1
                blow = (state[sid]['lives'] == 0)
                if blow:
                    state[sid]['eliminated'] = True
                    state[sid]['elim_round'] = round_idx
                credits.append((round_idx, pursuer_sid, sid, blow))

            # Mod-flagged elimination without a corresponding life-loss is
            # rare but theoretically possible. Sync defensively.
            if mod_eliminated and not state[sid]['eliminated']:
                state[sid]['eliminated'] = True
                state[sid]['elim_round'] = round_idx

    return state, credits, last_seen_lives, rounds_played, observed_names


# ── Placement ────────────────────────────────────────────────────────────

def assign_placements(state, lobby_sids):
    survivors = [s for s in lobby_sids if not state[s]['eliminated']]
    eliminated = [s for s in lobby_sids if state[s]['eliminated']]
    placements = {s: 1 for s in survivors}
    by_round = defaultdict(list)
    for s in eliminated:
        by_round[state[s]['elim_round']].append(s)
    next_rank = len(survivors) + 1
    for r in sorted(by_round.keys(), reverse=True):
        bucket = by_round[r]
        for s in bucket:
            placements[s] = next_rank
        next_rank += len(bucket)
    return placements


# ── Format flag ──────────────────────────────────────────────────────────

def format_flag(rounds_total, unique_maps):
    if unique_maps == rounds_total:
        return 'roulette'
    if unique_maps not in (rounds_total, 4):
        return 'non_standard'
    return 'standard'


# ── Per-cup build ────────────────────────────────────────────────────────

def build_cup(event, date, data):
    rounds = data['rounds']
    lives = data['settings'].get('lives', 3)
    state, credits, last_seen_lives, rounds_played, observed_names = derive_cup(data)

    lobby_sids = list(state.keys())
    placements = assign_placements(state, lobby_sids)

    # Per-cup tally
    tags_made = defaultdict(int)
    tagged_by = defaultdict(int)
    elims_made = defaultdict(int)
    eliminated_by = {}  # sid -> sid (the one who delivered the killing blow)
    tags_on = defaultdict(lambda: defaultdict(int))
    tags_received_from = defaultdict(lambda: defaultdict(int))
    elims_on = defaultdict(lambda: defaultdict(int))
    elimd_by = defaultdict(lambda: defaultdict(int))

    for round_idx, tagger, victim, was_blow in credits:
        tagged_by[victim] += 1
        tags_received_from[victim][tagger] += 1
        if was_blow:
            elims_made[tagger] += 1
            elims_on[tagger][victim] += 1
            elimd_by[victim][tagger] += 1
            eliminated_by[victim] = tagger
        else:
            tags_made[tagger] += 1
            tags_on[tagger][victim] += 1

    # Note: by spec we count tags_made as non-blow tag credits and elims_made
    # as blow credits, so tags_made + elims_made = total credits-as-tagger.

    # Maps array
    maps_arr = []
    seen_uids = set()
    for r in rounds:
        lvl = r.get('level', {})
        uid = lvl.get('UID', '')
        maps_arr.append({
            'round': r.get('roundNumber', 0),
            'name': lvl.get('Name', ''),
            'author': lvl.get('Author', ''),
            'uid': uid,
        })
        seen_uids.add(uid)
    fmt = format_flag(len(rounds), len(seen_uids))

    # Winner (placement == 1, but there may be ties — pick the lone alive)
    winner_sid = None
    for sid in lobby_sids:
        if not state[sid]['eliminated']:
            if winner_sid is None:
                winner_sid = sid
            else:
                # multiple survivors — pick the one with most lives
                if state[sid]['lives'] > state[winner_sid]['lives']:
                    winner_sid = sid

    # Per-player results within this cup
    results = []
    for sid in lobby_sids:
        canonical_name = resolve(str(sid), observed_names[sid])
        results.append({
            'steamid': str(sid),
            'name': canonical_name,
            'placement': placements[sid],
            'elim_round': state[sid]['elim_round'],
            'rounds_played': rounds_played[sid],
            'lives_lost': lives - state[sid]['lives'],
            'tags_made': tags_made[sid],
            'tagged_by': tagged_by[sid],
            'eliminations_made': elims_made[sid],
            'eliminated_by_steamid': str(eliminated_by[sid]) if sid in eliminated_by else None,
        })
    results.sort(key=lambda r: (r['placement'], r['name'].lower()))

    cup_obj = {
        'event': event,
        'date': date,
        'stub': False,
        'format': fmt,
        'rounds_total': len(rounds),
        'lobby_size': len(lobby_sids),
        'lives': lives,
        'maps': maps_arr,
        'winner_steamid': str(winner_sid) if winner_sid else None,
        'winner_name': resolve(str(winner_sid), observed_names[winner_sid]) if winner_sid else None,
        'results': results,
    }

    # Surface internals for player rollup + verification (popped before write)
    cup_obj['_internals'] = {
        'state': state,
        'last_seen_lives': last_seen_lives,
        'tags_on': {k: dict(v) for k, v in tags_on.items()},
        'tags_received_from': {k: dict(v) for k, v in tags_received_from.items()},
        'elims_on': {k: dict(v) for k, v in elims_on.items()},
        'elimd_by': {k: dict(v) for k, v in elimd_by.items()},
        'observed_names': dict(observed_names),
        'placements': placements,
        'rounds_played': dict(rounds_played),
        'lives_start': lives,
        'rounds': rounds,  # raw round data for ELO pair extraction
    }
    return cup_obj


def build_stub_cup(event, meta):
    return {
        'event': event,
        'date': meta['date'],
        'stub': True,
        'mapper_name': meta['mapper'],
        'winner_name': meta['winner'],
    }


# ── Per-cup point computation ────────────────────────────────────────────

def cup_points(placement):
    idx = placement - 1
    return PLACEMENT_PTS[idx] if idx < len(PLACEMENT_PTS) else 0


# ── Player rollups ───────────────────────────────────────────────────────

def build_players(cups_real):
    """Given the non-stub cups, build per-player aggregate."""
    by_sid = {}
    aliases_by_sid = defaultdict(set)
    tag_matrix = defaultdict(lambda: {
        'tags_on': defaultdict(int),
        'tagged_by': defaultdict(int),
        'elims_on': defaultdict(int),
        'elimd_by': defaultdict(int),
    })

    # First pass: collect all observed names per sid
    for cup in cups_real:
        intr = cup['_internals']
        for sid, names in intr['observed_names'].items():
            for n in names:
                if n:
                    aliases_by_sid[sid].add(n)

    # Initialize empty player records
    for sid in aliases_by_sid:
        canonical = resolve(str(sid), list(aliases_by_sid[sid]))
        by_sid[sid] = {
            'steamid': str(sid),
            'name': canonical,
            'aliases': sorted(a for a in aliases_by_sid[sid] if a != canonical),
            'cups_attended': 0,
            'cups_won': 0,
            'podiums': {'gold': 0, 'silver': 0, 'bronze': 0},
            'rounds_played_total': 0,
            'lives_lost_total': 0,
            'tags_made_total': 0,
            'tagged_by_total': 0,
            'eliminations_made_total': 0,
            'times_eliminated_total': 0,
            'history': [],
        }

    # Second pass: aggregate per cup
    for cup in cups_real:
        intr = cup['_internals']
        results_by_sid = {int(r['steamid']): r for r in cup['results']}
        for sid, p in by_sid.items():
            r = results_by_sid.get(sid)
            if r is None:
                continue
            p['cups_attended'] += 1
            placement = r['placement']
            if placement == 1:
                p['cups_won'] += 1
                p['podiums']['gold'] += 1
            elif placement == 2:
                p['podiums']['silver'] += 1
            elif placement == 3:
                p['podiums']['bronze'] += 1
            p['rounds_played_total'] += r['rounds_played']
            p['lives_lost_total'] += r['lives_lost']
            p['tags_made_total'] += r['tags_made']
            p['tagged_by_total'] += r['tagged_by']
            p['eliminations_made_total'] += r['eliminations_made']
            if r['elim_round'] is not None:
                p['times_eliminated_total'] += 1
            pts = cup_points(placement)
            p['history'].append({
                'event': cup['event'],
                'placement': placement,
                'elim_round': r['elim_round'],
                'tags': r['tags_made'],
                'tagged': r['tagged_by'],
                'elims': r['eliminations_made'],
                'points': pts,
            })

        # Tag matrix entries
        for tagger_sid, vmap in intr['tags_on'].items():
            for victim_sid, cnt in vmap.items():
                tag_matrix[tagger_sid]['tags_on'][victim_sid] += cnt
                tag_matrix[victim_sid]['tagged_by'][tagger_sid] += cnt
        for tagger_sid, vmap in intr['elims_on'].items():
            for victim_sid, cnt in vmap.items():
                tag_matrix[tagger_sid]['elims_on'][victim_sid] += cnt
                tag_matrix[victim_sid]['elimd_by'][tagger_sid] += cnt

    # Attach tag_matrix and finalize
    out = []
    for sid, p in by_sid.items():
        m = tag_matrix.get(sid, {})
        p['tag_matrix'] = {
            'tags_on':   {str(k): v for k, v in dict(m.get('tags_on', {})).items()},
            'tagged_by': {str(k): v for k, v in dict(m.get('tagged_by', {})).items()},
            'elims_on':  {str(k): v for k, v in dict(m.get('elims_on', {})).items()},
            'elimd_by':  {str(k): v for k, v in dict(m.get('elimd_by', {})).items()},
        }
        out.append(p)

    out.sort(key=lambda p: (-p['cups_won'], -p['cups_attended'], p['name'].lower()))
    return out


# ── Ranking ──────────────────────────────────────────────────────────────

def compute_ranking(players, cups_real):
    """ATP-style: rolling window of last RANK_WINDOW non-stub events,
    best RANK_BEST_OF per-cup scores count."""
    if not cups_real:
        return {'scheme': 'atp_hybrid_v1', 'players': []}

    event_nums = sorted({c['event'] for c in cups_real})
    window_events = set(event_nums[-RANK_WINDOW:])
    window_first = min(window_events)
    window_last = max(window_events)

    ranking_players = []
    for p in players:
        in_window = [h for h in p['history'] if h['event'] in window_events]
        if not in_window:
            continue
        in_window_sorted = sorted(in_window, key=lambda h: -h['points'])
        for i, h in enumerate(in_window_sorted):
            h['counted'] = i < RANK_BEST_OF
        total = sum(h['points'] for h in in_window_sorted if h['counted'])
        total_all = sum(h['points'] for h in in_window_sorted)
        counted = sum(1 for h in in_window_sorted if h['counted'])
        dropped = len(in_window_sorted) - counted

        # Window-scoped wins / podiums
        window_wins = sum(1 for h in in_window if h['placement'] == 1)
        window_podiums = {
            'gold':   sum(1 for h in in_window if h['placement'] == 1),
            'silver': sum(1 for h in in_window if h['placement'] == 2),
            'bronze': sum(1 for h in in_window if h['placement'] == 3),
        }

        ranking_players.append({
            'steamid': p['steamid'],
            'name': p['name'],
            'points': total,
            'points_all': total_all,
            'cups_in_window': len(in_window),
            'wins': window_wins,
            'podiums': window_podiums,
            'counted': counted,
            'dropped': dropped,
            'history': sorted(in_window_sorted, key=lambda h: h['event']),
        })

    ranking_players.sort(key=lambda r: (
        -r['points'], -r['wins'],
        -(r['podiums']['gold'] + r['podiums']['silver'] + r['podiums']['bronze']),
    ))
    for i, r in enumerate(ranking_players):
        r['rank'] = i + 1

    return {
        'scheme': 'placement_only_v2',
        'window': RANK_WINDOW,
        'best_of': RANK_BEST_OF,
        'window_first_event': window_first,
        'window_last_event': window_last,
        'params': {
            'placement_pts': PLACEMENT_PTS,
            'beyond_max_place': 0,
        },
        'players': ranking_players,
    }


# ── ELO ranking (v2: pair-based, single variant) ───────────────────────

def _expected(ra, rb):
    return 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))


def compute_elo_ranking(cups_real):
    """Pair-based ELO across all non-stub cups. TyO is literally pairwise
    competition, so a lobby-quality weighting would double-count — we just
    use straight ELO with provisional bonus and no decay."""
    if not cups_real:
        return {'scheme': 'elo_pair_v1', 'players': []}

    cups_sorted = sorted(cups_real, key=lambda c: c['event'])

    ratings = {}
    peak    = {}
    gp      = defaultdict(int)
    pair_w  = defaultdict(int)
    pair_l  = defaultdict(int)
    pair_d  = defaultdict(int)
    pairs_played = defaultdict(int)
    history = defaultdict(list)
    podiums = defaultdict(lambda: {'gold': 0, 'silver': 0, 'bronze': 0})
    name_for = {}
    self_target_warnings = []
    total_pairs = 0

    def r_(sid): return ratings.get(sid, ELO_STARTING)

    for cup in cups_sorted:
        intr = cup.get('_internals') or {}
        rounds = intr.get('rounds') or []
        if not rounds:
            continue
        evt = cup['event']

        placement_by_sid = {int(r['steamid']): r['placement'] for r in cup['results']}
        for r in cup['results']:
            name_for[int(r['steamid'])] = r['name']

        lobby_sids = set()
        for rnd in rounds:
            for pr in rnd['playerResults']:
                lobby_sids.add(pr['steamID'])

        deltas = defaultdict(float)
        cup_pairs_per_sid = defaultdict(int)
        cup_w = defaultdict(int)
        cup_l = defaultdict(int)
        cup_d = defaultdict(int)
        cup_pair_count = 0

        for rnd in rounds:
            results = {pr['steamID']: pr for pr in rnd['playerResults']}
            for sid_a, pr in results.items():
                if pr.get('eliminated') or pr.get('spectator'):
                    continue
                sid_b = pr.get('targetSteamID')
                if sid_b is None or sid_b == sid_a:
                    if sid_b == sid_a:
                        self_target_warnings.append(f"event {evt} sid {sid_a}: self-target")
                    continue
                pr_b = results.get(sid_b)
                if pr_b is None:
                    continue
                if pr_b.get('eliminated') or pr_b.get('spectator'):
                    continue

                ta = pr.get('time')
                tb = pr_b.get('time')
                a_dnf = (ta == 'NaN')
                b_dnf = (tb == 'NaN')
                if a_dnf and b_dnf:
                    s = 0.5
                elif a_dnf:
                    s = 0.0
                elif b_dnf:
                    s = 1.0
                else:
                    if ta < tb:   s = 1.0
                    elif ta > tb: s = 0.0
                    else:         s = 0.5

                ra = r_(sid_a); rb = r_(sid_b)
                ea = _expected(ra, rb)
                base_a = ELO_K_PAIR * (s       - ea)
                base_b = ELO_K_PAIR * ((1 - s) - (1 - ea))
                if gp[sid_a] < ELO_PROV_CUPS: base_a *= ELO_PROV_MULT
                if gp[sid_b] < ELO_PROV_CUPS: base_b *= ELO_PROV_MULT
                deltas[sid_a] += base_a
                deltas[sid_b] += base_b

                cup_pairs_per_sid[sid_a] += 1
                cup_pairs_per_sid[sid_b] += 1
                if s == 1.0:
                    cup_w[sid_a] += 1; cup_l[sid_b] += 1
                elif s == 0.0:
                    cup_l[sid_a] += 1; cup_w[sid_b] += 1
                else:
                    cup_d[sid_a] += 1; cup_d[sid_b] += 1
                cup_pair_count += 1

        # Apply deltas at end of cup
        for sid in lobby_sids:
            ratings[sid] = r_(sid) + deltas.get(sid, 0.0)
            peak[sid] = max(peak.get(sid, ELO_STARTING), ratings[sid])

        # gp + per-cup counters + history (only for sids with ≥1 non-skipped pair)
        for sid in lobby_sids:
            if cup_pairs_per_sid.get(sid, 0) == 0:
                continue
            gp[sid] += 1
            pair_w[sid] += cup_w[sid]
            pair_l[sid] += cup_l[sid]
            pair_d[sid] += cup_d[sid]
            pairs_played[sid] += cup_pairs_per_sid[sid]
            placement = placement_by_sid.get(sid)
            entry = {'e': evt, 'r': round(ratings[sid], 1),
                     'w': cup_w[sid], 'l': cup_l[sid], 'd': cup_d[sid]}
            if placement is not None:
                entry['p'] = placement
                if placement == 1:   podiums[sid]['gold']   += 1
                elif placement == 2: podiums[sid]['silver'] += 1
                elif placement == 3: podiums[sid]['bronze'] += 1
            history[sid].append(entry)

        total_pairs += cup_pair_count

    rows = []
    for sid, rating in ratings.items():
        if gp[sid] < ELO_MIN_CUPS_DISPLAY:
            continue
        rows.append({
            'steamid': str(sid),
            'name': name_for.get(sid, f'Unknown({sid})'),
            'rating': round(rating, 1),
            'peak': round(peak.get(sid, ELO_STARTING), 1),
            'cups': gp[sid],
            'pairs': pairs_played[sid],
            'wins': pair_w[sid],
            'losses': pair_l[sid],
            'draws': pair_d[sid],
            'gold': podiums[sid]['gold'],
            'silver': podiums[sid]['silver'],
            'bronze': podiums[sid]['bronze'],
            'history': history[sid],
        })
    rows.sort(key=lambda x: (-x['rating'], -x['cups'], x['name'].lower()))
    for i, row in enumerate(rows):
        row['rank'] = i + 1

    return {
        'scheme': 'elo_pair_v1',
        'params': {
            'starting': ELO_STARTING,
            'k_pair': ELO_K_PAIR,
            'prov_cups': ELO_PROV_CUPS,
            'prov_mult': ELO_PROV_MULT,
            'decay': None,
            'min_cups_display': ELO_MIN_CUPS_DISPLAY,
        },
        'first_event': cups_sorted[0]['event'],
        'last_event': cups_sorted[-1]['event'],
        'total_pairs': total_pairs,
        '_unfiltered_totals': {
            'wins': sum(pair_w.values()),
            'losses': sum(pair_l.values()),
            'draws': sum(pair_d.values()),
        },
        'self_target_warnings': self_target_warnings,
        'players': rows,
    }


# ── Verification ─────────────────────────────────────────────────────────

def verify(cups_real, players, events_meta):
    warnings = []
    fmt_counts = Counter()

    for cup in cups_real:
        evt = cup['event']
        intr = cup['_internals']
        fmt_counts[cup['format']] += 1

        # 3. Single winner
        ones = [r for r in cup['results'] if r['placement'] == 1]
        if len(ones) != 1:
            warnings.append(f"event {evt}: {len(ones)} players at placement=1 (expected 1)")

        # 4. Lives accounting
        for r in cup['results']:
            sid = int(r['steamid'])
            mod_lives = intr['last_seen_lives'].get(sid, intr['lives_start'])
            derived_lost = r['lives_lost']
            mod_lost = intr['lives_start'] - mod_lives
            if derived_lost != mod_lost:
                warnings.append(
                    f"event {evt} sid {sid} ({r['name']}): "
                    f"derived lives_lost={derived_lost}, mod says {mod_lost}"
                )

        # 5. Tag credit conservation
        sum_tags = sum(r['tags_made'] for r in cup['results'])
        sum_elims = sum(r['eliminations_made'] for r in cup['results'])
        sum_tagged = sum(r['tagged_by'] for r in cup['results'])
        if sum_tags + sum_elims != sum_tagged:
            warnings.append(
                f"event {evt}: tag credits not conserved: "
                f"tagger side {sum_tags + sum_elims} vs victim side {sum_tagged}"
            )

        # 6. Elim conservation
        survivors = sum(1 for r in cup['results'] if r['placement'] == 1)
        elimd = sum(1 for r in cup['results'] if r['elim_round'] is not None)
        if elimd != cup['lobby_size'] - survivors:
            warnings.append(
                f"event {evt}: elim count mismatch (elimd={elimd}, expected="
                f"{cup['lobby_size'] - survivors})"
            )
        if sum_elims != elimd:
            warnings.append(
                f"event {evt}: eliminations_made sum ({sum_elims}) != "
                f"distinct eliminated players ({elimd})"
            )

        # 7. Every player has a placement
        for r in cup['results']:
            if r['placement'] is None:
                warnings.append(f"event {evt} sid {r['steamid']}: no placement assigned")

    # 8. Cross-check derived winner against events.md
    cross_mismatches = []
    for cup in cups_real:
        meta = events_meta.get(cup['event'])
        if not meta:
            continue
        # Loose comparison: tag-stripped + canonical
        derived = cup['winner_name'] or ''
        expected = meta['winner']
        d_canon = NAME_MAP.get(derived, derived)
        d_stripped = re.sub(r'\[.*?\]\s*', '', derived).strip()
        d_canon2 = NAME_MAP.get(d_stripped, d_stripped)
        e_canon = NAME_MAP.get(expected, expected)
        if e_canon not in (d_canon, d_canon2, derived, d_stripped):
            cross_mismatches.append(f"  event {cup['event']}: derived={derived!r}, events.md={expected!r}")

    return warnings, cross_mismatches, fmt_counts


def verify_elo(ranking_elo, players, cups_real):
    """Run ELO sanity checks. Returns warnings list."""
    import math
    warnings = []
    n_cups = len(cups_real)

    for v in ranking_elo.get('self_target_warnings', []):
        warnings.append(f"ELO {v}")

    # Pair conservation on UNFILTERED totals (filter excludes <2-cup players)
    totals = ranking_elo.get('_unfiltered_totals', {})
    sum_w = totals.get('wins', 0)
    sum_l = totals.get('losses', 0)
    sum_d = totals.get('draws', 0)
    if sum_w != sum_l:
        warnings.append(f"ELO conservation: sum(wins)={sum_w} != sum(losses)={sum_l}")
    if sum_d % 2 != 0:
        warnings.append(f"ELO conservation: sum(draws)={sum_d} is odd")

    for r in ranking_elo.get('players', []):
        if not math.isfinite(r['rating']):
            warnings.append(f"ELO {r['name']}: non-finite rating {r['rating']}")
        if r['rating'] < 1000 or r['rating'] > 2200:
            warnings.append(f"ELO {r['name']}: rating {r['rating']} outside sanity bounds (1000-2200)")
        if r['pairs'] != r['wins'] + r['losses'] + r['draws']:
            warnings.append(
                f"ELO {r['name']}: pairs={r['pairs']} != "
                f"w+l+d={r['wins'] + r['losses'] + r['draws']}"
            )
        if r['peak'] < r['rating'] - 1e-3:
            warnings.append(f"ELO {r['name']}: peak {r['peak']} < rating {r['rating']}")
        if r['cups'] > n_cups:
            warnings.append(f"ELO {r['name']}: cups={r['cups']} > total cups {n_cups}")
    return warnings


# ── Main ────────────────────────────────────────────────────────────────

def main():
    events_meta = parse_events_md(EVENTS_MD)
    log_entries = load_logs(LOGS_DIR)

    # Build cups (real + stubs)
    cups_real = []
    for event, date, data, _path in log_entries:
        cups_real.append(build_cup(event, date, data))

    real_event_nums = {c['event'] for c in cups_real}
    stub_cups = []
    for event, meta in events_meta.items():
        if event not in real_event_nums and event >= min(real_event_nums) - 1 \
                and event <= max(real_event_nums) + 1:
            # Only stub events directly adjacent to the log range. Tweak as
            # backfill arrives. Currently catches 22 and 26.
            stub_cups.append(build_stub_cup(event, meta))

    cups_combined = cups_real + stub_cups
    cups_combined.sort(key=lambda c: c['event'])

    # Players + rankings
    players = build_players(cups_real)
    ranking = compute_ranking(players, cups_real)
    ranking_elo = compute_elo_ranking(cups_real)

    # Verification
    warnings, cross_mismatches, fmt_counts = verify(cups_real, players, events_meta)
    elo_warnings = verify_elo(ranking_elo, players, cups_real)
    warnings.extend(elo_warnings)

    # Strip internals before serializing
    for cup in cups_combined:
        if '_internals' in cup:
            del cup['_internals']
    # Don't ship internal-only fields inside the JSON — keep payload clean
    _strip_internal = {'self_target_warnings', '_unfiltered_totals'}
    ranking_elo_out = {k: v for k, v in ranking_elo.items() if k not in _strip_internal}

    out = {
        'meta': {
            'generated': dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'total_cups': len(cups_real),
            'stub_cups': len(stub_cups),
            'total_players': len(players),
            'last_event': max((c['event'] for c in cups_combined), default=None),
            'ranking_window': RANK_WINDOW,
            'ranking_best_of': RANK_BEST_OF,
        },
        'cups': cups_combined,
        'players': players,
        'ranking': ranking,
        'ranking_elo': ranking_elo_out,
    }

    with open(OUT_JSON, 'w', encoding='utf-8') as fp:
        json.dump(out, fp, ensure_ascii=False, indent=2)

    # 10. Round-trip parse check
    with open(OUT_JSON, encoding='utf-8') as fp:
        json.load(fp)

    # ── Summary ─────────────────────────────────────────────────────────
    print(f'\nBuilt {os.path.basename(OUT_JSON)}')
    n_real = len(cups_real)
    print(f'  {n_real} cups (' + ', '.join(
        f'{cnt} {fmt}' for fmt, cnt in fmt_counts.most_common()
    ) + f') + {len(stub_cups)} stubs (' + ', '.join(
        f'#{c["event"]}' for c in stub_cups
    ) + ')')
    total_entries = sum(c.get('lobby_size', 0) for c in cups_real)
    print(f'  {len(players)} players, {total_entries} player-cup entries')
    print(f'  Tag-credit + elim conservation: '
          f'{n_real - sum(1 for w in warnings if "conserv" in w or "elim count" in w)}/{n_real} pass')
    n_compared = sum(1 for c in cups_real if c['event'] in events_meta)
    if cross_mismatches:
        print(f'  Winner-vs-events.md: {n_compared - len(cross_mismatches)}/{n_compared} match')
        print('    Mismatches:')
        for m in cross_mismatches:
            print(m)
    elif n_compared:
        print(f'  Winner-vs-events.md: {n_compared}/{n_compared} match')
    else:
        print('  Winner-vs-events.md: no overlap (logs and table cover different events)')
    if ranking['players']:
        print(f"  Ranking window: events {ranking['window_first_event']}-"
              f"{ranking['window_last_event']} ({ranking['window']}), "
              f"best-of-{ranking['best_of']}")

    # ELO summary
    n_elo = len(ranking_elo.get('players', []))
    pairs = ranking_elo.get('total_pairs', 0)
    totals = ranking_elo.get('_unfiltered_totals', {})
    consv = (totals.get('wins') == totals.get('losses') and totals.get('draws', 0) % 2 == 0)
    print(f"  ELO ranking: {n_elo} displayed (cups >= {ELO_MIN_CUPS_DISPLAY}), "
          f"{pairs} total pairs, conservation: {'pass' if consv else 'FAIL'}")
    if ranking_elo.get('players'):
        top5 = ranking_elo['players'][:5]
        print('    Top 5: ' + ' · '.join(f"{r['name']} {r['rating']}" for r in top5))

    if warnings:
        print(f'\nWarnings ({len(warnings)}):')
        for w in warnings[:60]:
            print(f'  {w}')
        if len(warnings) > 60:
            print(f'  ... and {len(warnings) - 60} more')
    else:
        print('\nNo warnings.')


if __name__ == '__main__':
    main()
