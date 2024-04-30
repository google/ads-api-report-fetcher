from __future__ import annotations

import glob
import pathlib
import subprocess

import pytest

_QUERIES_PATH = pathlib.Path(__file__).resolve().parent / 'data'


@pytest.mark.e2e
def test_gaarf_version():
    result = subprocess.run(['gaarf', '--version'],
                            capture_output=True,
                            text=True)
    assert not result.stderr
    assert 'gaarf version' in result.stdout


@pytest.mark.skip('Failing for FieldError')
@pytest.mark.e2e
def test_gaarf_console_input():
    result = subprocess.run([
        'gaarf',
        '"SELECT customer.id FROM customer LIMIT 1"',
        '--input=console',
    ],
                            capture_output=True,
                            text=True)
    assert not result.stderr


@pytest.mark.e2e
def test_gaarf_many_queries():
    queries = glob.glob(f'{_QUERIES_PATH}/*.sql')
    result = subprocess.run(
        ['gaarf'] + queries + [
            '--output=console',
        ],
        capture_output=True,
        text=True)
    out = result.stdout
    assert not result.stderr
    for query in queries:
        assert f'{query} executed successfully' in out
