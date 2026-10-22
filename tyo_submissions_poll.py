"""tyo_submissions_poll.py: publish TyO cups submitted on submit.html.

Run by the GitHub Action (.github/workflows/process-submission.yml) every 15
minutes in the TyO window (Saturday 18:00 to Sunday 11:45 UTC). Each pass:

  1. Asks the tyo-submit Worker for pending submissions.
  2. Marks 'processed' cups (open pull request) that are now on main as
     'published'.
  3. For 'received' ones, oldest cup first, AT MOST ONE cup per pass:
       - cup already on main: the same file -> 'published' (a pass that died
         after pushing), the same rounds -> 'duplicate', different rounds ->
         'duplicate' with a REVIEW note and a red run (GitHub emails aizpun);
       - several files for one cup: the newest wins (agix correcting his own
         upload), the others are 'superseded';
       - runs process_tyo.py --ci on the file. Clean -> committed straight to
         main ('published'). Review flags (format, numbering, date) -> branch
         tyo-<N> plus a pull request ('processed'); merging it publishes.
         A failure -> 'failed' with the error and a red run.
     While a tyo-<N> branch exists and cup N is not on main, later cups wait:
     their snapshot and ratings build on it. To reject a pull request, close
     it and delete its branch.

No local state: the Worker's records are the state, so a pass that dies
half-way is repaired by the next one.

Config from the environment: TYO_WORKER_URL, TYO_POLLER_TOKEN, GH_TOKEN (for
the gh CLI). Downloads go to submissions/ (gitignored).

Usage:
  python tyo_submissions_poll.py [--once] [--ci] [--dry-run] [--no-push]
                                 [--worker URL] [--repo DIR] [--id ID] [--reindex] [-v]
"""
import argparse
import datetime
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROCESS_TIMEOUT_S = 900
LOG_RE = re.compile(r'^TYO_(\d+)_(\d{8})\.json$')

log = logging.getLogger('tyo_submissions_poll')


class Ctx:
    def __init__(self, args):
        self.worker = (args.worker or os.environ.get('TYO_WORKER_URL') or '').rstrip('/')
        self.token = os.environ.get('TYO_POLLER_TOKEN') or ''
        self.repo = os.path.abspath(args.repo or HERE)
        self.ci = args.ci
        self.dry_run = args.dry_run
        self.no_push = args.no_push
        self.only_id = args.id
        self.reindex = args.reindex
        self.remote, self.branch = 'origin', 'main'
        self.subdir = os.path.join(self.repo, 'submissions')
        os.makedirs(self.subdir, exist_ok=True)
        self.exit_code = 0


def setup_logging(verbose):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        pass
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S'))
    log.addHandler(h)
    log.setLevel(logging.DEBUG if verbose else logging.INFO)


# ── Worker API ───────────────────────────────────────────────────────

class WorkerError(Exception):
    pass


def _ssl_context():
    """certifi when available: the Windows store can fail on workers.dev chains."""
    import ssl
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


SSL_CTX = _ssl_context()


def api(ctx, method, path, body=None, raw=False, timeout=30):
    data = None
    # Cloudflare answers 403 to Python's default User-Agent.
    headers = {'Authorization': 'Bearer ' + ctx.token, 'User-Agent': 'tyo-submissions-poll'}
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(ctx.worker + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            payload = r.read()
            return (payload, dict(r.headers)) if raw else json.loads(payload.decode('utf-8'))
    except urllib.error.HTTPError as e:
        raise WorkerError(f'{method} {path}: HTTP {e.code} {e.read().decode("utf-8", "replace")[:300]}')
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise WorkerError(f'{method} {path}: {e}')


def post_status(ctx, sub_id, status, note='', summary=None):
    body = {'status': status, 'note': note[:500]}
    if summary is not None:
        body['summary'] = summary
    if ctx.dry_run:
        log.info('[dry-run] would POST /status/%s %s %s', sub_id, status, note)
        return
    try:
        api(ctx, 'POST', '/status/' + sub_id, body)
        log.info('status %s -> %s %s', sub_id, status, note)
    except WorkerError as e:
        # The next pass sees the record unchanged and decides again.
        log.warning('could not post status for %s: %s', sub_id, e)


def download(ctx, sub):
    path = os.path.join(ctx.subdir, f"{sub['id']}.json")
    data, headers = api(ctx, 'GET', '/log/' + sub['id'], raw=True, timeout=120)
    got = hashlib.sha256(data).hexdigest()
    want = sub.get('sha256') or headers.get('X-Sha256') or headers.get('x-sha256')
    if want and got != want:
        raise WorkerError(f"sha256 mismatch for {sub['id']}: {got} != {want}")
    with open(path, 'wb') as f:
        f.write(data)
    return path, data


# ── git ──────────────────────────────────────────────────────────────

def git(ctx, *a, check=True):
    r = subprocess.run(['git', *a], cwd=ctx.repo, capture_output=True, text=True,
                       encoding='utf-8', errors='replace', timeout=120)
    if check and r.returncode != 0:
        raise RuntimeError(f"git {' '.join(a)}: {r.stderr.strip() or r.stdout.strip()}")
    return r.stdout.strip()


def cup_branch(event):
    return f'tyo-{event}'


def main_logs(ctx):
    """{event: (filename, bytes)} for every log on the remote main branch."""
    ref = f'{ctx.remote}/{ctx.branch}'
    out = {}
    for name in git(ctx, 'ls-tree', '--name-only', ref, 'logs/').splitlines():
        m = LOG_RE.match(os.path.basename(name))
        if m:
            blob = subprocess.run(['git', 'show', f'{ref}:{name}'], cwd=ctx.repo,
                                  capture_output=True, timeout=60, check=True).stdout
            out[int(m.group(1))] = (os.path.basename(name), blob)
    return out


def open_branches(ctx):
    """Events with a tyo-<N> branch on the remote (a pull request waiting)."""
    out = git(ctx, 'ls-remote', '--heads', ctx.remote, 'tyo-*', check=False)
    return sorted(int(m.group(1)) for m in re.finditer(r'refs/heads/tyo-(\d+)$', out, re.M))


# ── processing ───────────────────────────────────────────────────────

def worker_summary(s):
    """The fields the Worker's status table keeps (see its cleanSummary)."""
    return {'winner': s.get('winner'), 'podium': [{'name': n} for n in s.get('podium', [])],
            'players': s.get('lobby_size'), 'rounds': s.get('rounds_total'),
            'warnings': s.get('flags', [])}


def commit_message(sub, s):
    title = f"Ingest cup {s['event']} ({s['date']}): {s['winner']} won."
    who = sub.get('submitter') or 'the submit page'
    return f"{title}\n\nSubmitted by {who} (submission {sub['id']}); processed by the GitHub Action."


def pr_body(sub, s):
    lines = [f"Submitted by {sub.get('submitter') or 'someone'} on the site (submission `{sub['id']}`).",
             '', '**Needs a look before publishing:**', ''] + [f'- {f}' for f in s['flags']]
    lines += ['', f"- Winner: **{s['winner']}**, {s['lobby_size']} players, {s['rounds_total']} rounds ({s['format']})",
              '- Podium: ' + ', '.join(s['podium']),
              '- Maps: ' + ', '.join(s['maps'])]
    if s['new_players']:
        lines += ['', '**New players**', '']
        for p in s['new_players']:
            reg = {True: '', False: ' (not in players.json)', None: ''}[p['in_registry']]
            lines.append(f"- {p['name']} `{p['steamid']}`{reg}")
    lines += ['', 'Merging publishes the cup. SOF and the cross-comp ratings catch up when '
                  '`catch_up.py` runs on the PC.']
    return '\n'.join(lines)


def publish_or_pr(ctx, sub, s, log_name):
    """Commit the ingest. No flags: straight to main. Flags: a pull request."""
    paths = ['tyo.json', 'snapshot.json', f'logs/{log_name}', 'old snapshots']
    if s['flags']:
        git(ctx, 'checkout', '-b', cup_branch(s['event']))
    git(ctx, 'add', '--', *paths)
    git(ctx, 'commit', '-m', commit_message(sub, s))
    if ctx.no_push:
        log.info('[no-push] committed locally on %s', git(ctx, 'rev-parse', '--abbrev-ref', 'HEAD'))
        return 'processed' if s['flags'] else 'published', 'committed locally (--no-push)'
    if not s['flags']:
        git(ctx, 'push', ctx.remote, f'HEAD:{ctx.branch}')
        request_pages_build(ctx)
        return 'published', 'live on the site'
    git(ctx, 'push', ctx.remote, cup_branch(s['event']))
    title = f"TyO cup {s['event']} ({s['date']}): needs a look"
    r = subprocess.run(['gh', 'pr', 'create', '--base', ctx.branch, '--head', cup_branch(s['event']),
                        '--title', title, '--body', pr_body(sub, s)],
                       cwd=ctx.repo, capture_output=True, text=True, encoding='utf-8', timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f'gh pr create: {r.stderr.strip()}')
    return 'processed', 'waiting for aizpun: ' + r.stdout.strip().splitlines()[-1]


def request_pages_build(ctx):
    """Best effort: a push made with GITHUB_TOKEN may not start a Pages build."""
    repo = os.environ.get('GITHUB_REPOSITORY')
    if not repo:
        return
    r = subprocess.run(['gh', 'api', '-X', 'POST', f'repos/{repo}/pages/builds'],
                       cwd=ctx.repo, capture_output=True, text=True, timeout=60)
    log.info('pages build request: %s', 'ok' if r.returncode == 0 else r.stderr.strip()[:200])


def run_cup(ctx, sub, path):
    """process_tyo.py --ci on the file, then publish or open a pull request."""
    event, date = sub['event'], sub['date'].replace('-', '')
    log_name = f'TYO_{event}_{date}.json'
    staged = os.path.join(ctx.subdir, 'in', log_name)
    os.makedirs(os.path.dirname(staged), exist_ok=True)
    with open(path, 'rb') as src, open(staged, 'wb') as dst:
        dst.write(src.read())
    summary_path = os.path.join(ctx.subdir, f"{sub['id']}.summary.json")
    cmd = [sys.executable, os.path.join(ctx.repo, 'process_tyo.py'), '--ci', staged,
           '--summary', summary_path]
    log.info('running: %s', subprocess.list2cmdline(cmd))
    if ctx.dry_run:
        log.info('[dry-run] process_tyo.py not run')
        return
    try:
        r = subprocess.run(cmd, cwd=ctx.repo, capture_output=True, text=True, encoding='utf-8',
                           errors='replace', timeout=PROCESS_TIMEOUT_S,
                           env=dict(os.environ, PYTHONIOENCODING='utf-8'))
    except subprocess.TimeoutExpired:
        post_status(ctx, sub['id'], 'failed', 'processing timed out; aizpun must check it')
        ctx.exit_code = 1
        return
    out = (r.stdout or '') + (r.stderr or '')
    with open(os.path.join(ctx.subdir, f"{sub['id']}.out"), 'w', encoding='utf-8') as f:
        f.write(out)
    print(out)
    if r.returncode != 0:
        errors = [l.strip()[len('error:'):].strip() for l in out.splitlines() if l.strip().startswith('error:')]
        post_status(ctx, sub['id'], 'failed', errors[-1] if errors else f'process_tyo.py exited {r.returncode}')
        ctx.exit_code = 1
        return
    with open(summary_path, encoding='utf-8') as f:
        s = json.load(f)
    try:
        status, note = publish_or_pr(ctx, sub, s, log_name)
    except (RuntimeError, subprocess.SubprocessError, OSError) as e:
        post_status(ctx, sub['id'], 'failed', f'processed, but publishing failed: {e}')
        ctx.exit_code = 1
        return
    post_status(ctx, sub['id'], status, note, worker_summary(s))


def one_pass(ctx):
    if not ctx.worker or not ctx.token:
        log.error('TYO_WORKER_URL / TYO_POLLER_TOKEN missing')
        return 2
    # Two writes landing together can drop a record from the Worker's index,
    # and /pending reads the index. Rebuild it in the first pass of each hour
    # (about 18 times a weekend; KV list + write limits are per day).
    if ctx.reindex or datetime.datetime.now(datetime.timezone.utc).minute < 15:
        if ctx.dry_run:
            log.info('[dry-run] would POST /reindex')
        else:
            try:
                log.info('reindex: %s', api(ctx, 'POST', '/reindex'))
            except WorkerError as e:
                log.warning('reindex failed: %s', e)
    try:
        pending = api(ctx, 'GET', '/pending').get('submissions', [])
    except WorkerError as e:
        log.warning('worker unreachable, nothing done: %s', e)
        return 0
    if ctx.only_id:
        pending = [s for s in pending if s['id'] == ctx.only_id]
    log.info('%d pending submission(s)', len(pending))
    if not pending:
        return 0

    git(ctx, 'fetch', ctx.remote, '--quiet')
    on_main = main_logs(ctx)

    for sub in pending:
        if sub['status'] == 'processed' and sub['event'] in on_main:
            post_status(ctx, sub['id'], 'published', 'live on the site', sub.get('summary'))

    received = sorted((s for s in pending if s['status'] == 'received'), key=lambda s: s['created'])
    by_event = {}
    for sub in received:
        by_event.setdefault(sub['event'], []).append(sub)

    sys.path.insert(0, ctx.repo)
    from process_tyo import cup_fingerprint  # noqa: E402  (repo module, imported after the path is set)

    # A tyo-<N> branch whose cup is not on main is a pull request waiting.
    # Rejecting one = close the PR and delete its branch.
    waiting = [e for e in open_branches(ctx) if e not in on_main]
    for event in sorted(by_event):
        subs = by_event[event]
        best = subs[-1]  # newest: agix re-sending is a correction
        for sub in subs[:-1]:
            post_status(ctx, sub['id'], 'superseded', f"a newer file for cup {event} arrived ({best['id']})")
        try:
            path, data = download(ctx, best)
        except (WorkerError, OSError) as e:
            log.warning('download failed for %s: %s', best['id'], e)
            continue

        if event in on_main:
            name, blob = on_main[event]
            if hashlib.sha256(blob).hexdigest() == hashlib.sha256(data).hexdigest():
                post_status(ctx, best['id'], 'published', 'live on the site')
                continue
            try:
                same = cup_fingerprint(json.loads(data.decode('utf-8-sig'))) == \
                       cup_fingerprint(json.loads(blob.decode('utf-8-sig')))
            except (ValueError, KeyError):
                same = False
            if same:
                post_status(ctx, best['id'], 'duplicate',
                            f'cup {event} is already on the site; this file matches the published result')
            else:
                post_status(ctx, best['id'], 'duplicate',
                            f'REVIEW: cup {event} is already on the site with different results ({name}); '
                            'aizpun must compare them by hand')
                ctx.exit_code = 1
            continue

        if waiting:
            log.info('cup %s waits: the pull request for cup %s must be merged first', event, min(waiting))
            return 0
        run_cup(ctx, best, path)
        return 0  # one cup per pass: the next one builds on this commit
    return 0


def main():
    ap = argparse.ArgumentParser(description='Publish TyO cups submitted on submit.html.')
    ap.add_argument('--once', action='store_true', help='one pass (the default and only mode)')
    ap.add_argument('--ci', action='store_true', help='GitHub Action mode')
    ap.add_argument('--dry-run', action='store_true', help='decide and log, change nothing')
    ap.add_argument('--no-push', action='store_true', help='commit locally, never push or open a PR (rehearsals)')
    ap.add_argument('--worker', help='override TYO_WORKER_URL')
    ap.add_argument('--repo', help='repo to process in (default: this folder)')
    ap.add_argument('--id', help='only handle this submission id')
    ap.add_argument('--reindex', action='store_true', help='rebuild the Worker index first, whatever the time')
    ap.add_argument('-v', '--verbose', action='store_true')
    args = ap.parse_args()
    setup_logging(args.verbose)
    ctx = Ctx(args)
    rc = one_pass(ctx)
    sys.exit(rc or ctx.exit_code)


if __name__ == '__main__':
    main()
