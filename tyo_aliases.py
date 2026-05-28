"""Name resolution for TyO. SteamID is the primary key; this module only
decides which display string to associate with each steamID."""
import ast, os, re
from collections import Counter

_dir = os.path.dirname(os.path.abspath(__file__))
_elo_engine = os.path.join(os.path.dirname(_dir), 'zeepkist cotd elo', 'elo_engine.py')

# Pull CANONICAL out of elo_engine.py via AST so we don't trigger its COTD
# pipeline (which parses xlsx files that may be in flux on aizpun's machine).
CANONICAL: dict[str, list[str]] = {}
try:
    _tree = ast.parse(open(_elo_engine, encoding='utf-8').read())
    for _node in _tree.body:
        if (isinstance(_node, ast.Assign)
                and len(_node.targets) == 1
                and isinstance(_node.targets[0], ast.Name)
                and _node.targets[0].id == 'CANONICAL'):
            CANONICAL = ast.literal_eval(_node.value)
            break
except (OSError, SyntaxError, ValueError):
    pass

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
