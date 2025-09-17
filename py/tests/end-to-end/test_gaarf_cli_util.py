# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

from __future__ import annotations

import glob
import os
import pathlib
import subprocess

import pytest

_QUERIES_PATH = pathlib.Path(__file__).resolve().parent / 'data'
_TEST_ADS_ACCOUNT = os.getenv('TEST_ADS_ACCOUNT')


@pytest.mark.e2e
def test_gaarf_version():
  result = subprocess.run(
    'gaarf --version', capture_output=True, text=True, shell=True, check=False
  )
  assert result.returncode == 0
  assert 'gaarf version' in str(result.stdout)


@pytest.mark.e2e
def test_gaarf_console_input():
  query = 'SELECT customer.id FROM customer LIMIT 1'
  command = f'gaarf "{query}" --input console'
  result = subprocess.run(
    command,
    capture_output=True,
    shell=True,
    check=False,
  )
  assert result.returncode == 0
  assert f'{query} executed successfully' in str(result.stdout)


@pytest.mark.e2e
def test_gaarf_many_queries():
  queries = glob.glob(f'{_QUERIES_PATH}/*.sql')
  command = (
    f'gaarf {' '.join(queries)} '
    '--output=console '
    f'--account={_TEST_ADS_ACCOUNT} '
    '--disable-account-expansion'
  )
  result = subprocess.run(
    command,
    capture_output=True,
    text=True,
    shell=True,
    check=False,
  )
  assert result.returncode == 0
  out = str(result.stdout)
  assert 'Running queries in parallel' in out
  assert 'Skipping account expansion' in out
  for query in queries:
    assert f'{query} executed successfully' in out


@pytest.mark.e2e
def test_gaarf_no_queries():
  command = 'gaarf --output console'
  result = subprocess.run(
    command,
    capture_output=True,
    text=True,
    shell=True,
    check=False,
  )
  assert result.returncode == 1
  assert 'GaarfMissingQueryException' in result.stderr
  assert 'Please provide one or more queries to run' in result.stderr


@pytest.mark.e2e
def test_gaarf_raises_error_on_invalid_google_ads_config():
  command = (
    'gaarf "SELECT customer.id FROM customer LIMIT 1" '
    '--ads-config=missing-google-ads.yaml'
  )
  result = subprocess.run(
    command,
    capture_output=True,
    text=True,
    shell=True,
    check=False,
  )

  assert result.returncode == 1


@pytest.mark.e2e
def test_gaarf_runs_queries_sequentially():
  queries = glob.glob(f'{_QUERIES_PATH}/*.sql')
  command = (
    f'gaarf {' '.join(queries)} '
    '--output=console '
    f'--account={_TEST_ADS_ACCOUNT} '
    '--no-parallel-queries'
  )
  result = subprocess.run(
    command,
    capture_output=True,
    text=True,
    shell=True,
    check=False,
  )
  assert result.returncode == 0
  out = str(result.stdout)
  assert 'Running queries sequentially' in result.stdout
  for query in queries:
    assert f'{query} executed successfully' in out


@pytest.mark.e2e
def test_gaarf_only_saves_config_during_dry_run(tmp_path):
  tmp_config_path = tmp_path / 'config.yaml'
  queries = glob.glob(f'{_QUERIES_PATH}/*.sql')
  command = (
    f'gaarf {' '.join(queries)} --output=console --dry-run',
    '--save-config',
    f'--config-destination={tmp_config_path}',
  )
  result = subprocess.run(
    command,
    capture_output=True,
    text=True,
    shell=True,
    check=False,
  )
  out = str(result.stdout)
  assert result.returncode == 0
  assert 'initialized config' not in out
  assert 'Running queries' not in out
