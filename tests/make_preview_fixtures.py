"""Write tests/fixtures/preview_expected.json from tyo.json.

The in-browser preview (tyo_preview.js) must agree with build_tyo.py on every
logged cup: winner, lobby size, rounds, format and each player's placement.
This file is that answer key. Rerun after a rule change in build_tyo.py:

    python tests/make_preview_fixtures.py
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, 'tests', 'fixtures', 'preview_expected.json')


def main():
    with open(os.path.join(ROOT, 'tyo.json'), encoding='utf-8') as fp:
        tyo = json.load(fp)
    expected = {}
    for cup in sorted(tyo['cups'], key=lambda c: c['event']):
        if cup.get('stub'):
            continue
        expected[str(cup['event'])] = {
            'winner_steamid': cup['winner_steamid'],
            'lobby_size': cup['lobby_size'],
            'rounds_total': cup['rounds_total'],
            'format': cup['format'],
            'placements': {str(r['steamid']): r['placement'] for r in cup['results']},
        }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8', newline='\n') as fp:
        json.dump(expected, fp, indent=1, sort_keys=True)
        fp.write('\n')
    print(f'{len(expected)} cups -> {os.path.relpath(OUT, ROOT)}')


if __name__ == '__main__':
    main()
