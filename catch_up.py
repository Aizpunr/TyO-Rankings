"""catch_up.py: run on the PC after cups were published online.

The GitHub Action (tyo_submissions_poll.py) publishes a cup without the PC,
so the cup goes live without its cross-comp SOF (the site shows a dash). The
cross-comp data lives only on this PC, so this script finishes the job:

  1. git pull --ff-only (main)
  2. cross-comp refresh (zeepkist holistic/refresh.py; a GTR timeout is
     non-fatal, same as process_tyo.py)
  3. build_tyo.py + the usual verification, which attaches the new SOF
  4. commits tyo.json if anything changed ("SOF backfill ...")

With --push it also pushes that commit and the SOF repo's elo_pool.json +
docs/allcompdata.json. Without it, nothing leaves the PC.

  python catch_up.py [--no-pull] [--push]
"""
import json
import sys

import process_tyo as pt


def git(*a, cwd=pt.ROOT):
    return pt.run(['git', *a], cwd=cwd)


def pull() -> bool:
    branch = git('rev-parse', '--abbrev-ref', 'HEAD').stdout.strip()
    if branch != 'main':
        print(f'error: on branch {branch!r}, not main; switch first.')
        return False
    if git('status', '--porcelain', '--', 'tyo.json', 'snapshot.json', 'logs').stdout.strip():
        print('error: tyo.json, snapshot.json or logs/ has local changes; commit or stash them first.')
        return False
    r = git('pull', '--ff-only')
    if r.returncode != 0:
        print(f'error: git pull --ff-only failed: {r.stderr.strip()}')
        return False
    print(r.stdout.strip() or 'up to date')
    return True


def sof_by_event(text: str) -> dict:
    return {c['event']: c.get('sof') for c in json.loads(text)['cups'] if not c.get('stub')}


def main() -> int:
    if '--no-pull' not in sys.argv and not pull():
        return 1
    before = sof_by_event(git('show', 'HEAD:tyo.json').stdout)

    print('\ncross-comp refresh...')
    if not pt.crosscomp_refresh():
        print('error: cross-comp refresh did not produce SOF; nothing to backfill.')
        return 1

    print('rebuilding tyo.json...')
    pt.verify_build_output(pt.run_build())
    after = sof_by_event(pt.TYO_JSON.read_text(encoding='utf-8'))

    filled = sorted(e for e in after if before.get(e) is None and after[e] is not None)
    if not git('status', '--porcelain', '--', 'tyo.json').stdout.strip():
        print('tyo.json unchanged: nothing to commit.')
    else:
        if filled:
            msg = 'SOF backfill: ' + ', '.join(f'cup {e} SOF {after[e]:.1f}' for e in filled) + '.'
        else:
            msg = 'Refresh cross-comp SOF.'
        git('add', 'tyo.json')
        r = git('commit', '-m', msg)
        if r.returncode != 0:
            print(f'error: git commit failed: {r.stderr or r.stdout}')
            return 2
        print(f'committed: {msg}')

    if '--push' not in sys.argv:
        print('\nnot pushed. To publish: git push here, then commit + push elo_pool.json and '
              'docs/allcompdata.json in the SOF repo (or use --push next time).')
        return 0

    r = git('push')
    print('pushed TyO' if r.returncode == 0 else f'error: TyO push failed: {r.stderr.strip()}')
    if pt.SOF_MOD_REPO.exists():
        git('add', 'elo_pool.json', 'docs/allcompdata.json', cwd=pt.SOF_MOD_REPO)
        c = git('commit', '-m', 'Refresh SOF pool + docs after TyO catch-up', cwd=pt.SOF_MOD_REPO)
        if c.returncode == 0:
            p = git('push', cwd=pt.SOF_MOD_REPO)
            print('pushed SOF repo' if p.returncode == 0 else f'error: SOF repo push failed: {p.stderr.strip()}')
        else:
            print('SOF repo: nothing to push')
    return 0


if __name__ == '__main__':
    sys.exit(main())
