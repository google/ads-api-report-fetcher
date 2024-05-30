from __future__ import annotations

from gaarf.executors import bq_executor


def test_extract_datasets():
  macros = {
    'start_date': ':YYYYMMDD',
    'bq_dataset': 'dataset_1',
    'dataset_new': 'dataset_2',
    'legacy_dataset_old': 'dataset_3',
    'wrong_dts': 'dataset_4',
  }

  expected = [
    'dataset_1',
    'dataset_2',
    'dataset_3',
  ]
  datasets = bq_executor.extract_datasets(macros)
  assert datasets == expected
