"""The ingest guards in process_tyo.py that protect auto-publish.

Run: python -m pytest tests -q
"""
import datetime as dt
import json
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import process_tyo as pt  # noqa: E402

LOGS = sorted(ROOT.glob('logs/TYO_*.json'), key=lambda p: int(pt.LOG_RE.match(p.name).group(1)))


def last_cup():
    cups = [c for c in json.loads((ROOT / 'tyo.json').read_text(encoding='utf-8'))['cups'] if not c.get('stub')]
    return max(cups, key=lambda c: c['event'])


def next_saturday_after(date_iso):
    d = dt.date.fromisoformat(date_iso) + dt.timedelta(days=1)
    while d.weekday() != 5:
        d += dt.timedelta(days=1)
    return d.strftime('%Y%m%d')


def test_fingerprint_same_file_equal_and_cups_distinct():
    fps = [pt.cup_fingerprint(pt.load_log(p)) for p in LOGS]
    assert pt.cup_fingerprint(pt.load_log(LOGS[-1])) == fps[-1]
    assert len(set(fps)) == len(fps)


def test_resent_previous_cup_is_refused(tmp_path, monkeypatch):
    logs = tmp_path / 'logs'
    logs.mkdir()
    shutil.copy2(LOGS[-1], logs / LOGS[-1].name)
    monkeypatch.setattr(pt, 'LOGS', logs)
    resent = tmp_path / 'TYO_999_20991231.json'
    shutil.copy2(LOGS[-1], resent)
    with pytest.raises(SystemExit) as e:
        pt.refuse_if_stale(resent, pt.load_log(resent))
    assert e.value.code == 1


def test_resume_of_the_same_file_is_not_stale(tmp_path, monkeypatch):
    logs = tmp_path / 'logs'
    logs.mkdir()
    own = logs / LOGS[-1].name
    shutil.copy2(LOGS[-1], own)
    monkeypatch.setattr(pt, 'LOGS', logs)
    pt.refuse_if_stale(own, pt.load_log(own))


def test_every_logged_cup_has_one_survivor():
    for p in LOGS:
        pt.refuse_if_unfinished(pt.load_log(p))


def test_unfinished_cup_is_refused():
    data = pt.load_log(LOGS[-1])
    data['rounds'] = data['rounds'][:3]
    with pytest.raises(SystemExit) as e:
        pt.refuse_if_unfinished(data)
    assert e.value.code == 1


def test_no_flags_for_the_expected_next_cup():
    last = last_cup()
    data = pt.load_log(LOGS[-1])
    assert pt.review_flags(data, last['event'] + 1, next_saturday_after(last['date'])) == []


def test_flags_number_gap_weekday_and_date_order():
    last = last_cup()
    data = pt.load_log(LOGS[-1])
    sat = next_saturday_after(last['date'])
    friday = (dt.datetime.strptime(sat, '%Y%m%d').date() - dt.timedelta(days=1)).strftime('%Y%m%d')
    before = (dt.date.fromisoformat(last['date']) - dt.timedelta(days=7)).strftime('%Y%m%d')
    assert any('does not follow' in f for f in pt.review_flags(data, last['event'] + 2, sat))
    assert any('Friday' in f for f in pt.review_flags(data, last['event'] + 1, friday))
    assert any('not after' in f for f in pt.review_flags(data, last['event'] + 1, before))


def test_long_gap_alone_is_not_flagged():
    # Skipped Saturdays are normal; only the cup number signals a missing cup.
    last = last_cup()
    data = pt.load_log(LOGS[-1])
    four_weeks = (dt.date.fromisoformat(last['date']) + dt.timedelta(days=28)).strftime('%Y%m%d')
    assert pt.review_flags(data, last['event'] + 1, four_weeks) == []


def test_roulette_format_is_flagged():
    roulette = next(p for p in LOGS if pt.LOG_RE.match(p.name).group(1) == '25')
    last = last_cup()
    flags = pt.review_flags(pt.load_log(roulette), last['event'] + 1, next_saturday_after(last['date']))
    assert any('roulette' in f for f in flags)
