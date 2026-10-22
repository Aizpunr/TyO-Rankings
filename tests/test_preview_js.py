"""Run the browser preview's JavaScript fixture and rule checks under Node."""
import os
import shutil
import subprocess

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE = shutil.which('node')

pytestmark = pytest.mark.skipif(NODE is None, reason='node not installed')


def test_js_preview_matches_fixtures():
    result = subprocess.run(
        [NODE, 'tests/js/run_preview_fixtures.mjs'], cwd=REPO,
        capture_output=True, encoding='utf-8', errors='replace', timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
