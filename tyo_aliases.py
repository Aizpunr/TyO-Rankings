"""Name resolution for TyO. SteamID is the primary key; this module only
decides which display string to associate with each steamID."""
import os, sys, re, io
from collections import Counter

_dir = os.path.dirname(os.path.abspath(__file__))
elo_dir = os.path.join(os.path.dirname(_dir), 'zeepkist cotd elo')
sys.path.insert(0, elo_dir)

# elo_engine.py runs its full COTD pipeline at import time (no __main__ guard)
# and calls sys.stdout.reconfigure, so we need a real text stream that supports
# reconfigure. Redirect to a TextIOWrapper over BytesIO and discard.
_real_stdout = sys.stdout
sys.stdout = io.TextIOWrapper(io.BytesIO(), encoding='utf-8', write_through=True)
try:
    from elo_engine import CANONICAL  # type: ignore
finally:
    sys.stdout = _real_stdout

# Reverse lookup: alias -> canonical
NAME_MAP = {}
for canonical, aliases in CANONICAL.items():
    NAME_MAP[canonical] = canonical
    for a in aliases:
        NAME_MAP[a] = canonical

# Project-local overrides keyed by steamID (string). Use only when the
# automatic resolution picks the wrong display name for a given account.
TYO_ALIASES: dict[str, str] = {}


def _strip_tag(name: str) -> str:
    return re.sub(r'\[.*?\]\s*', '', name).strip()


def resolve(steamid: str, observed_names: list[str]) -> str:
    """Return canonical display name for a steamID given all usernames seen
    for that steamID across logs. Resolution order:
      1. TYO_ALIASES[steamid] override
      2. any observed (or tag-stripped) name that maps to a CANONICAL key
      3. most-frequent observed name (ties broken by latest occurrence)
    Empty strings are ignored.
    """
    if steamid in TYO_ALIASES:
        return TYO_ALIASES[steamid]

    cleaned = [n for n in observed_names if n]
    if not cleaned:
        return f'Unknown({steamid})'

    # Try CANONICAL match on raw or tag-stripped form
    for n in cleaned:
        if n in NAME_MAP:
            return NAME_MAP[n]
        stripped = _strip_tag(n)
        if stripped and stripped in NAME_MAP:
            return NAME_MAP[stripped]

    # Most-frequent, latest-wins on tie
    counts = Counter(cleaned)
    top = counts.most_common(1)[0][1]
    candidates = [n for n in counts if counts[n] == top]
    # Latest occurrence among ties
    for n in reversed(cleaned):
        if n in candidates:
            return n
    return cleaned[-1]
