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
from __future__ import annotations

import pytest

from gaarf import bq_executor


class TestBigQueryExecutor:
  def test_instantiating_bq_executor_is_deprecated(self):
    with pytest.warns(DeprecationWarning) as w:
      bq_executor.BigQueryExecutor(project_id='fake-project')
      assert len(w) == 1
      assert issubclass(w[0].category, DeprecationWarning)
      assert str(w[0].message) == (
        'Loading BigQueryExecutor from `gaarf.bq_executor` is '
        'deprecated; Import BigQueryExecutor from '
        '`gaarf.executors.bq_executor` instead'
      )


def test_extract_datasets_is_deprecated():
  with pytest.warns(DeprecationWarning) as w:
    bq_executor.extract_datasets({})
    assert len(w) == 1
    assert issubclass(w[0].category, DeprecationWarning)
    assert str(w[0].message) == (
      'Loading `extract_datasets` from `gaarf.bq_executor` is '
      'deprecated; Import `extract_datasets` from '
      '`gaarf.executors.bq_executor` instead'
    )
