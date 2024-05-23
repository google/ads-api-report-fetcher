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
    assert 'Running queries in parallel' in out
    for query in queries:
        assert f'{query} executed successfully' in out


@pytest.mark.e2e
def test_gaarf_no_queries():
    result = subprocess.run([
        'gaarf',
        '--output=console',
    ],
                            capture_output=True,
                            text=True)
    assert 'GaarfMissingQueryException' in result.stderr
    assert 'Please provide one or more queries to run' in result.stderr


@pytest.mark.e2e
def test_gaarf_raises_error_on_invalid_google_ads_config():
    result = subprocess.run([
        'gaarf',
        '"SELECT customer.id FROM customer LIMIT 1"',
        f'--ads-config={_QUERIES_PATH}/test-google-ads.yaml',
    ],
                            capture_output=True,
                            text=True)

    assert result.stderr


@pytest.mark.e2e
def test_gaarf_runs_queries_sequentially():
    queries = glob.glob(f'{_QUERIES_PATH}/*.sql')
    result = subprocess.run(
        ['gaarf'] + queries + [
            '--output=console',
            '--no-parallel-queries',
        ],
        capture_output=True,
        text=True)
    assert 'Running queries sequentially' in result.stdout


@pytest.mark.e2e
def test_gaarf_only_saves_config_during_dry_run(tmp_path):
    tmp_config_path = tmp_path / 'config.yaml'
    queries = glob.glob(f'{_QUERIES_PATH}/*.sql')
    result = subprocess.run(
        ['gaarf'] + queries + [
            '--output=console',
            '--dry-run',
            '--save-config',
            f'--config-destination={tmp_config_path}',
        ],
        capture_output=True,
        text=True)
    out = result.stdout
    assert not result.stderr
    assert 'initialized config' not in out
    assert 'Running queries' not in out
