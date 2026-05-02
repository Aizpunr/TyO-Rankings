"""snapshot.py — capture current Season 2 + All Time rankings into
snapshot.json so the next cup's leaderboard shows ▲/▼ rank arrows and
score deltas. Run after each cup is processed (after build_tyo.py).

Usage: python snapshot.py
"""
import json
import os
import shutil
from datetime import datetime

base = os.path.dirname(os.path.abspath(__file__))
def _p(f): return os.path.join(base, f)


with open(_p('tyo.json'), encoding='utf-8') as f:
    data = json.load(f)

real_events = [c['event'] for c in data['cups'] if not c.get('stub')]
current_cup = max(real_events) if real_events else 0

# Backup the existing snapshot to old snapshots/ before we overwrite it
snap_path = _p('snapshot.json')
backup_dir = _p('old snapshots')
if os.path.exists(snap_path):
    with open(snap_path, encoding='utf-8') as fp:
        old = json.load(fp)
    old_cup = (old.get('_meta') or {}).get('event', 'unknown')
    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(backup_dir, f'snapshot {old_cup}.json')
    i = 0
    while os.path.exists(backup_path):
        i += 1
        backup_path = os.path.join(backup_dir, f'snapshot {old_cup}_{i}.json')
    shutil.copy2(snap_path, backup_path)
    print(f'Backed up -> old snapshots/{os.path.basename(backup_path)}')


def snap_ranking(rows, score_field):
    """Per-player [rank, score, wins, pods] keyed by steamid.

    Both ranking shapes are accepted: ranking.players have podiums nested
    under `podiums`, ranking_elo.players expose gold/silver/bronze flat.
    """
    out = {}
    for r in rows:
        pod = r.get('podiums') or {
            'gold': r.get('gold', 0),
            'silver': r.get('silver', 0),
            'bronze': r.get('bronze', 0),
        }
        pods_total = pod.get('gold', 0) + pod.get('silver', 0) + pod.get('bronze', 0)
        wins = pod.get('gold', 0)
        out[r['steamid']] = [r['rank'], r[score_field], wins, pods_total]
    return out


snap = {
    '_meta':  {
        'event': current_cup,
        'generated': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    },
    'season': snap_ranking(data['ranking']['players'],     'points'),
    'elo':    snap_ranking(data['ranking_elo']['players'], 'rating'),
}

tmp = snap_path + '.tmp'
with open(tmp, 'w', encoding='utf-8') as fp:
    json.dump(snap, fp, separators=(',', ':'))
os.replace(tmp, snap_path)

print(
    f'snapshot.json written (cup #{current_cup}, '
    f'season={len(snap["season"])}, elo={len(snap["elo"])})'
)
