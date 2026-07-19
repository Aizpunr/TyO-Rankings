"""process_tyo.py — single-shot ingest for a new TyO cup log.

Usage:
    python process_tyo.py                           # auto-discover from Downloads
    python process_tyo.py path/to/TYO_50_....json   # explicit path
    python process_tyo.py --push                    # after verifying on localhost,
                                                    # commits+pushes TyO + SOF mod repos

Full arc (mirrors COTD's new_cup.py where possible):
  1. Discover the log (arg or Downloads auto-pick)
  2. Validate JSON shape + filename
  3. Refuse if cup already in tyo.json (real ingest, not just log-in-logs/)
  4. Refuse if working tree is dirty (skip with --allow-dirty)
  5. Capture pre-cup snapshot
  6. Copy log into logs/
  7. Rebuild tyo.json
  8. Verify build summary (conservation pass, no unexpected warnings)
  9. Cross-comp refresh (best-effort — GTR La Liga timeout is non-fatal)
 10. SOF backfill via second build_tyo.py (if refresh succeeded)
 11. Print a movers report (winner + top-3 climbers/fallers per ranking)
 12. Start localhost on 9007 (if not up) + open browser for confirmation
 13. With --push: git commit+push TyO (ingest + SOF-backfill) AND SOF mod repo

If log is already in logs/ but cup not yet in tyo.json (interrupted prior run),
steps 5+6 are skipped and we resume at step 7. Explicit re-ingest still errors.

Exit codes: 0 = success, 1 = validation/business-rule failure,
2 = subprocess failure mid-pipeline.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOGS = ROOT / 'logs'
TYO_JSON = ROOT / 'tyo.json'
SNAP_JSON = ROOT / 'snapshot.json'
OLD_SNAPS = ROOT / 'old snapshots'

DOWNLOADS = Path.home() / 'Downloads'
CROSSCOMP_REFRESH = Path(r'C:\Users\rafa\Desktop\Claude\zeepkist holistic\refresh.py')
SOF_MOD_REPO = Path(r'C:\Users\rafa\Desktop\Claude\zeepkist mod\Zeepkist-Strength-of-Field')
LOCALHOST_PORT = 9007

# schmxrg (#11) is the only historic winner still unmatched — Hydro (#6) got
# matched at cup 48 when he first showed up in a logged lobby. Everything else
# in the "N/23 credited" warning list is a real problem.
KNOWN_HISTORIC_UNMATCHED = {11}

LOG_RE = re.compile(r'^TYO_(\d+)_(\d{8})\.json$')


# UTF-8 stdout/stderr so we can print arrows + em-dashes on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# ── Helpers ────────────────────────────────────────────────────────────

TOTAL_STEPS = 8  # displayed step counter (validate → localhost)


def step(n, msg):
    print(f'[{n}/{TOTAL_STEPS}] {msg}', end=' ', flush=True)


def ok(detail=''):
    print(f'ok  {detail}'.strip())


def warn(detail):
    print(f'warn  {detail}'.strip())


def die(code, msg, *, recovery=None):
    print(f'\nerror: {msg}', file=sys.stderr)
    if recovery:
        print(f'\nto recover: {recovery}', file=sys.stderr)
    sys.exit(code)


def run(cmd, *, cwd=ROOT, capture=True):
    """Run a subprocess. Returns CompletedProcess."""
    kw = dict(cwd=str(cwd), text=True, encoding='utf-8')
    if capture:
        kw['capture_output'] = True
    return subprocess.run(cmd, **kw)


def load_json(path: Path):
    with open(path, encoding='utf-8') as fp:
        return json.load(fp)


# ── Discovery ──────────────────────────────────────────────────────────

def load_ingested_events() -> set[int]:
    """Set of event numbers already present in tyo.json (real ingests only —
    stubs are excluded so a stub cup doesn't block a real re-ingest)."""
    if not TYO_JSON.exists():
        return set()
    data = load_json(TYO_JSON)
    return {c['event'] for c in data.get('cups', []) if not c.get('stub')}


def discover_log() -> Path:
    """Pick the newest TYO_*.json in Downloads that isn't in tyo.json yet.
    Fall back to logs/ (resume path) if nothing new in Downloads."""
    ingested = load_ingested_events()

    def not_ingested(candidates):
        pairs = []
        for p in candidates:
            m = LOG_RE.match(p.name)
            if m:
                n = int(m.group(1))
                if n not in ingested:
                    pairs.append((n, p))
        pairs.sort(key=lambda x: -x[0])
        return pairs

    dl_pairs = not_ingested(DOWNLOADS.glob('TYO_*.json'))
    if dl_pairs:
        return dl_pairs[0][1]

    logs_pairs = not_ingested(LOGS.glob('TYO_*.json'))
    if logs_pairs:
        return logs_pairs[0][1]

    die(1, f'no unin­gested TYO_*.json found in {DOWNLOADS} or {LOGS}.\n'
           f'  ingested cups so far: {sorted(ingested)[-5:] if ingested else []} ...')


# ── Validation ─────────────────────────────────────────────────────────

def validate_input(path: Path) -> tuple[int, str]:
    if not path.exists():
        die(1, f'{path} does not exist')
    m = LOG_RE.match(path.name)
    if not m:
        die(1, f'filename {path.name!r} does not match TYO_<n>_YYYYMMDD.json')
    event_num = int(m.group(1))
    date_str = m.group(2)
    try:
        dt.datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        die(1, f'date portion {date_str!r} is not a valid YYYYMMDD')

    try:
        with open(path, encoding='utf-8-sig') as fp:
            data = json.load(fp)
    except (OSError, json.JSONDecodeError) as e:
        die(1, f'cannot parse JSON: {e}')

    settings = data.get('settings') or {}
    if not isinstance(settings.get('lives'), int):
        die(1, 'settings.lives missing or not an int — does not look like a TyO log')
    rounds = data.get('rounds') or []
    if not isinstance(rounds, list) or not rounds:
        die(1, 'rounds[] missing or empty')
    for i, rnd in enumerate(rounds):
        if not (rnd.get('level') or {}).get('Name'):
            die(1, f'round {i}: level.Name missing')
        if not isinstance(rnd.get('playerResults'), list) or not rnd['playerResults']:
            die(1, f'round {i}: playerResults missing or empty')

    return event_num, date_str


def refuse_if_already_ingested(event_num: int):
    """Real re-ingest guard: refuse only if the cup is already in tyo.json.
    A log-in-logs/-but-not-in-tyo.json is a resume, not a re-ingest."""
    if event_num in load_ingested_events():
        die(1, f'cup {event_num} is already ingested (present in tyo.json).\n'
               f'  To force re-ingest: delete the row via a hand-edit, then rerun.')


def refuse_if_dirty(allow_dirty: bool):
    if allow_dirty:
        return
    r = run(['git', 'status', '--porcelain'])
    if r.returncode != 0:
        die(2, f'git status failed: {r.stderr.strip()}')
    if r.stdout.strip():
        die(1, 'working tree is dirty. Commit or stash first, or rerun with --allow-dirty.\n'
               + r.stdout.rstrip())


# ── Build steps ────────────────────────────────────────────────────────

def run_snapshot():
    r = run([sys.executable, str(ROOT / 'snapshot.py')])
    if r.returncode != 0:
        die(2, f'snapshot.py failed:\n{r.stderr or r.stdout}')


def run_build():
    r = run([sys.executable, str(ROOT / 'build_tyo.py')])
    if r.returncode != 0:
        die(2, f'build_tyo.py failed:\n{r.stderr or r.stdout}',
            recovery="git restore tyo.json snapshot.json 'old snapshots/' && "
                     'git clean -fd logs/')
    return r.stdout


def verify_build_output(stdout: str):
    if 'Tag-credit + elim conservation:' not in stdout:
        die(2, 'build output missing tag-credit conservation line')
    cons = re.search(r'Tag-credit \+ elim conservation:\s+(\d+)/(\d+) pass', stdout)
    if not cons or cons.group(1) != cons.group(2):
        die(2, f'tag-credit conservation did not pass:\n{stdout}')
    if 'conservation: pass' not in stdout:
        die(2, f'ELO conservation line not "pass":\n{stdout}')
    # Historic-wins unmatched entries: only allow the known ones
    unknown = []
    in_unmatched = False
    for line in stdout.splitlines():
        if 'Unmatched' in line:
            in_unmatched = True
            continue
        if in_unmatched and line.startswith('      event #'):
            m = re.match(r'\s*event #(\d+):', line)
            if m and int(m.group(1)) not in KNOWN_HISTORIC_UNMATCHED:
                unknown.append(line.strip())
        elif in_unmatched and not line.startswith('   '):
            in_unmatched = False
    if unknown:
        die(2, 'unexpected historic-wins unmatched:\n  ' + '\n  '.join(unknown))


def crosscomp_refresh() -> bool:
    """Run the cross-comp refresh. Returns True if SOF stages succeeded.
    GTR-API timeout in the last stage is non-fatal — allsofdata + allcompdata
    build first, so SOF is computed even if elo_pool.json can't refresh."""
    if not CROSSCOMP_REFRESH.exists():
        warn(f'refresh.py not found at {CROSSCOMP_REFRESH}')
        return False
    r = run([sys.executable, str(CROSSCOMP_REFRESH)],
            cwd=CROSSCOMP_REFRESH.parent)
    # Even on non-zero exit, SOF for the new cup may be available if the
    # first two stages (build_allsofdata + build_allcompdata) succeeded.
    tail = (r.stdout or '') + (r.stderr or '')
    stages_ok = 'build_allcompdata.py ---' in tail
    if r.returncode != 0:
        # Common case: GTR blocked (La Liga). Report but don't fail.
        if stages_ok:
            warn('cross-comp refresh: elo_pool step failed (GTR API likely blocked); '
                 'SOF still computed')
        else:
            warn(f'cross-comp refresh failed (returncode {r.returncode})')
        return stages_ok
    return True


def read_sof_for_event(event_num: int):
    """Look up the cross-comp SOF for this TyO event from allcompdata.json."""
    p = CROSSCOMP_REFRESH.parent / 'allcompdata.json'
    if not p.exists():
        return None
    d = load_json(p)
    for e in d.get('events', []):
        if e.get('comp') == 'tyo' and f'Event {event_num}' in str(e.get('id', '')):
            return e.get('sof')
    return None


# ── Movers report ──────────────────────────────────────────────────────

def movers(new_rows, prev_map, score_key, participants=None, top_n=5):
    """Compute rank/score deltas between snapshot and current standings.

    If `participants` (set of steamids) is given, restrict to those players —
    this is the natural scope for a per-cup report: only the players in the
    just-ingested lobby had their state directly changed. Without the filter,
    Season 2's rolling window ejects old results for absent players and their
    score-swings dominate the top-N. Sorted by |score_d| descending so the
    winner shows up even when their rank didn't change.
    """
    out = []
    for r in new_rows:
        sid = r['steamid']
        if participants is not None and sid not in participants:
            continue
        if sid not in prev_map:
            # Player has no snapshot entry — they didn't meet the display
            # cutoff (cups < 2) before this ingest. No meaningful "move".
            continue
        prev_rank, prev_score = prev_map[sid][0], prev_map[sid][1]
        new_rank = r['rank']
        new_score = r[score_key]
        rank_d = prev_rank - new_rank
        score_d = new_score - prev_score
        if rank_d == 0 and abs(score_d) < 0.5:
            continue
        out.append((sid, r['name'], rank_d, score_d, new_rank, prev_rank, new_score, prev_score))
    out.sort(key=lambda m: (-abs(m[3]), -abs(m[2])))
    return out[:top_n]


def fmt_mover(m, score_key):
    sid, name, rank_d, score_d, new_rank, prev_rank, new_score, prev_score = m
    arrow = '▲' if rank_d > 0 else ('▼' if rank_d < 0 else '·')
    rank_part = f'{arrow}{abs(rank_d)}' if rank_d else '·'
    if score_key == 'rating':
        score_part = f'{score_d:+.1f}'
    else:
        score_part = f'{int(score_d):+d}'
    prev_txt = f'#{prev_rank}→#{new_rank}' if prev_rank is not None else f'new #{new_rank}'
    return f'  {rank_part:<4} {name:<20} ({prev_txt}, {score_part})'


def print_report(event_num: int, date_str: str, sof: float | None):
    data = load_json(TYO_JSON)
    snap = load_json(SNAP_JSON)
    cup = next((c for c in data['cups'] if c.get('event') == event_num), None)
    if not cup:
        die(2, f'cup {event_num} not found in tyo.json after build')
    winner = cup.get('winner_name') or '?'
    pretty_date = f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}'
    print()
    line = f'cup {event_num} ingested ({pretty_date}). winner: {winner}'
    if sof is not None:
        line += f'. SOF: {sof:.1f}'
    line += f'. {cup.get("lobby_size", "?")} players'
    print(line)

    # Scope movers to this cup's participants — Season 2's rolling window
    # would otherwise dominate the list with off-cup drift.
    participants = {str(r['steamid']) for r in cup['results']}

    season_movers = movers(data['ranking']['players'], snap.get('season', {}),
                           'points', participants=participants)
    elo_movers = movers(data['ranking_elo']['players'], snap.get('elo', {}),
                        'rating', participants=participants)

    if season_movers:
        print('\nseason 2 movers:')
        for m in season_movers:
            print(fmt_mover(m, 'points'))
    if elo_movers:
        print('\nall time movers:')
        for m in elo_movers:
            print(fmt_mover(m, 'rating'))


# ── Localhost ──────────────────────────────────────────────────────────

def ensure_localhost():
    """Start detached http.server on LOCALHOST_PORT if not already listening."""
    try:
        with socket.create_connection(('127.0.0.1', LOCALHOST_PORT), timeout=0.3):
            return 'already serving'
    except OSError:
        pass
    flags = 0
    if hasattr(subprocess, 'DETACHED_PROCESS'):
        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(
        [sys.executable, '-m', 'http.server', str(LOCALHOST_PORT)],
        cwd=str(ROOT), creationflags=flags,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return 'started'


# ── Push ───────────────────────────────────────────────────────────────

def find_pending_cup() -> tuple[int, str, float | None]:
    """--push standalone: derive the not-yet-pushed cup from git state.

    Looks for an untracked/modified `logs/TYO_<N>_<date>.json` and takes that
    as the pending cup. Falls back to the highest cup in tyo.json if git is
    clean but tyo.json is ahead of HEAD (rare).
    """
    r = run(['git', 'status', '--porcelain', '--', 'logs/'])
    if r.returncode != 0:
        die(2, f'git status failed: {r.stderr}')
    pending = []
    for line in r.stdout.splitlines():
        # porcelain format: "XY path"
        path = line[3:].strip().strip('"')
        m = LOG_RE.match(Path(path).name)
        if m:
            pending.append(int(m.group(1)))
    if not pending:
        die(1, 'no pending TyO cup log found in git status (nothing to push).')
    event_num = max(pending)
    data = load_json(TYO_JSON)
    cup = next((c for c in data['cups'] if c.get('event') == event_num), None)
    if not cup:
        die(2, f'cup {event_num} log is untracked but not in tyo.json — '
               f'run process_tyo.py (without --push) first to build it.')
    date_str = None
    for line in r.stdout.splitlines():
        path = line[3:].strip().strip('"')
        m = LOG_RE.match(Path(path).name)
        if m and int(m.group(1)) == event_num:
            date_str = m.group(2)
            break
    return event_num, date_str, cup.get('sof')


def git_push(event_num: int, date_str: str, sof: float | None):
    data = load_json(TYO_JSON)
    cup = next(c for c in data['cups'] if c.get('event') == event_num)
    winner = cup.get('winner_name') or '?'
    pretty_date = f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}'

    # ── TyO repo: ingest commit (tyo.json + snapshot + log + old snapshot) ─
    ingest_paths = ['tyo.json', 'snapshot.json',
                    f'logs/TYO_{event_num}_{date_str}.json',
                    'old snapshots']
    add = run(['git', 'add', '--', *ingest_paths])
    if add.returncode != 0:
        die(2, f'git add failed:\n{add.stderr}')

    msg_parts = [f'Ingest cup {event_num} ({pretty_date}): {winner} won']
    if sof is not None:
        msg_parts.append(f'SOF {sof:.1f}')
    msg = '. '.join(msg_parts) + '.'
    commit = run(['git', 'commit', '-m', msg])
    if commit.returncode != 0 and 'nothing to commit' not in (commit.stdout or ''):
        die(2, f'git commit failed:\n{commit.stderr or commit.stdout}')

    push = run(['git', 'push'])
    if push.returncode != 0:
        die(2, f'git push failed:\n{push.stderr}')
    print(f'\npushed (TyO): {msg}')

    # ── SOF mod repo: elo_pool + docs refresh ──────────────────────────
    if sof is not None and SOF_MOD_REPO.exists():
        sof_add = run(['git', 'add', 'elo_pool.json', 'docs/allcompdata.json'],
                      cwd=SOF_MOD_REPO)
        if sof_add.returncode != 0:
            warn(f'SOF mod: git add failed — {sof_add.stderr}')
            return
        sof_msg = f'Refresh SOF pool + docs after TyO #{event_num}'
        sof_commit = run(['git', 'commit', '-m', sof_msg], cwd=SOF_MOD_REPO)
        if sof_commit.returncode != 0:
            if 'nothing to commit' in (sof_commit.stdout or ''):
                print('SOF mod: nothing to push (pool unchanged)')
                return
            warn(f'SOF mod: git commit failed — {sof_commit.stderr or sof_commit.stdout}')
            return
        sof_push = run(['git', 'push'], cwd=SOF_MOD_REPO)
        if sof_push.returncode != 0:
            warn(f'SOF mod: git push failed — {sof_push.stderr}')
            return
        print(f'pushed (SOF mod): {sof_msg}')


# ── Main ───────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='Ingest a new TyO cup log.')
    ap.add_argument('log_path', type=Path, nargs='?',
                    help='path to TYO_<n>_YYYYMMDD.json (default: auto-discover from Downloads)')
    ap.add_argument('--push', action='store_true',
                    help='second-invocation mode: skip build/refresh, just commit+push '
                         'the last-ingested cup to TyO + SOF mod repos')
    ap.add_argument('--allow-dirty', action='store_true',
                    help='skip the dirty-working-tree check (use sparingly)')
    ap.add_argument('--no-browser', action='store_true',
                    help='do not open the browser at the end')
    args = ap.parse_args()

    # ── --push mode: no build, just git ops on the pending cup ────────
    if args.push:
        event_num, date_str, sof = find_pending_cup()
        git_push(event_num, date_str, sof)
        return

    # ── Step 1: discover ──────────────────────────────────────────────
    step(1, 'discovering log...')
    src = args.log_path or discover_log()
    ok(f'({src.name})')

    # ── Step 2: validate ──────────────────────────────────────────────
    step(2, 'validating...')
    event_num, date_str = validate_input(src)
    refuse_if_already_ingested(event_num)
    ok(f'(event {event_num}, {date_str})')

    # ── Step 3: snapshot baseline ─────────────────────────────────────
    # Snapshot represents the standings BEFORE this ingest cycle. In the
    # normal path we snapshot right before adding the log; in the resume
    # path (log already in logs/) we snapshot only if the current snapshot
    # doesn't match the latest ingested cup in tyo.json — otherwise the
    # baseline is already correct and re-snapshotting just clutters
    # old snapshots/.
    canonical = LOGS / f'TYO_{event_num}_{date_str}.json'
    resuming = canonical.exists() and canonical.resolve() == src.resolve()
    if resuming:
        snap_event = None
        latest_ingested = max(load_ingested_events() or {0})
        if SNAP_JSON.exists():
            try:
                snap_event = load_json(SNAP_JSON).get('_meta', {}).get('event')
            except (OSError, json.JSONDecodeError):
                snap_event = None
        if snap_event == latest_ingested:
            step(3, 'snapshot already fresh (resume)...')
            ok(f'(meta.event={snap_event})')
        else:
            step(3, f'refreshing stale snapshot (was event {snap_event}, need {latest_ingested})...')
            run_snapshot()
            ok()
    else:
        step(3, 'snapshotting pre-cup baseline...')
        run_snapshot()
        ok()

    step(4, 'copying log to logs/...')
    if resuming:
        ok('(already in place)')
    else:
        if canonical.exists():
            # canonical exists but src is elsewhere → suspicious; refuse.
            die(1, f'{canonical} already exists but does not match {src}. '
                   f'Move the stray file aside before rerunning.')
        shutil.copy2(src, canonical)
        # Also remove the source copy from Downloads (safe: we now have canonical)
        if src.parent.resolve() == DOWNLOADS.resolve():
            try:
                src.unlink()
            except OSError:
                pass
        ok(f'-> logs/{canonical.name}')

    # ── Step 5: build ─────────────────────────────────────────────────
    step(5, 'rebuilding tyo.json...')
    build_out = run_build()
    m = re.search(r'^\s*(\d+) cups', build_out, re.MULTILINE)
    ok(f'({m.group(1)} cups)' if m else '')

    # ── Step 6: verify ────────────────────────────────────────────────
    step(6, 'verifying build...')
    verify_build_output(build_out)
    ok()

    # ── Step 7: cross-comp refresh + SOF backfill ─────────────────────
    step(7, 'cross-comp refresh + SOF backfill...')
    refresh_ok = crosscomp_refresh()
    sof = None
    if refresh_ok:
        sof = read_sof_for_event(event_num)
        if sof is not None:
            # Second build to attach SOF to tyo.json
            r = run([sys.executable, str(ROOT / 'build_tyo.py')])
            if r.returncode != 0:
                warn(f'second build (SOF backfill) failed:\n{r.stderr}')
                sof = None
            else:
                ok(f'(SOF {sof:.1f})')
        else:
            warn('SOF not yet in allcompdata.json — will retry on next run')
    else:
        warn('SOF backfill skipped (refresh not usable)')

    # ── Step 8: localhost + report ────────────────────────────────────
    step(8, 'localhost...')
    status = ensure_localhost()
    url = f'http://localhost:{LOCALHOST_PORT}/'
    ok(f'({status}) {url}')

    print_report(event_num, date_str, sof)

    if not args.no_browser:
        try:
            os.startfile(url)
        except (AttributeError, OSError):
            pass

    print()
    print(f'Verify on {url}, then:')
    print('  python process_tyo.py --push     # commits+pushes both repos')


if __name__ == '__main__':
    main()
