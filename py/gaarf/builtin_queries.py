# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.import proto
"""Defines queries that are accessible via `builtin` namespace.

Built-in queries provides quick access to important elements in Google Ads API
with a simple SQL syntax.
All built-in queries are available in `builtin` namespace
(i.e. `builtin.ocid_mapping`) and can be added to SQL scripts
(i.e. `SELECT * FROM builtin.ocid_mapping`).

"""

from __future__ import annotations

import functools
import operator
import re
from collections import abc, defaultdict
from typing import Callable

import gaarf


def get_ocid_mapping(
  report_fetcher: gaarf.report_fetcher.AdsReportFetcher, accounts: list[str]
) -> gaarf.report.GaarfReport:
  """Returns mapping between external customer_id and OCID parameter.


  OCID parameter is used to build links to Google Ads entities in UI.

  Args:
      report_fetcher: An instance of AdsReportFetcher.
      accounts: Google Ads accounts to get data on OCID mapping.

  Returns:
      Report with mapping between external customer_id and OCID parameter.
  """
  query = (
    'SELECT customer.id AS account_id, '
    'metrics.optimization_score_url  AS url FROM customer'
  )
  mapping = []
  for account in accounts:
    if report := report_fetcher.fetch(query, account):
      for row in report:
        if ocid := re.findall(r'ocid=(\w+)', row.url):
          mapping.append([row.account_id, ocid[0]])
          break
      if not ocid:
        mapping.append([int(account), '0'])
    else:
      mapping.append([int(account), '0'])
  return gaarf.report.GaarfReport(
    results=mapping,
    column_names=[
      'account_id',
      'ocid',
    ],
  )


def get_account_hierarchy_flattened(
  report_fetcher: gaarf.report_fetcher.AdsReportFetcher, accounts: list[str]
) -> gaarf.report.GaarfReport:
  SEED_ACCOUNTS_QUERY = """
    SELECT
        customer_client.level AS level,
        customer_client.manager AS is_manager,
        customer_client.id AS account_id
    FROM customer_client
    WHERE customer_client.level <= 1
    """

  NESTED_ACCOUNTS_QUERY = """
    SELECT
        customer.id AS mcc_id,
        customer.descriptive_name AS mcc_name,
        {level} AS level,
        customer_client_link.client_customer~0 AS account_id
    FROM customer_client_link
    """

  seed_accounts = (
    accounts if isinstance(accounts, abc.MutableSequence) else [accounts]
  )
  hierarchy = report_fetcher.fetch(SEED_ACCOUNTS_QUERY, seed_accounts)
  level_mapping = defaultdict(list)
  for row in hierarchy:
    if row.is_manager:
      level_mapping[row.level].append(row.account_id)

  reports: list[gaarf.report.GaarfReport] = []
  for level, accounts in level_mapping.items():
    if accounts:
      report_fetcher.customer_ids = accounts
      reports.append(
        report_fetcher.fetch(NESTED_ACCOUNTS_QUERY.format(level=level))
      )

  combined_report = functools.reduce(operator.add, reports)
  df = combined_report.to_pandas()
  max_level = max(df.level)
  root_mcc_df = df[df.level == 0][['mcc_id', 'mcc_name', 'account_id']]
  root_mcc_df = root_mcc_df.rename(
    columns={'mcc_id': 'root_mcc_id', 'mcc_name': 'root_mcc_name'}
  )
  root_mcc_df['account_id'] = root_mcc_df['account_id'].astype(int)
  selectable_columns = ['root_mcc_id', 'root_mcc_name']
  if max_level == 0:
    return gaarf.report.GaarfReport.from_pandas(root_mcc_df)
  else:
    for i in range(1, max(df.level) + 1):
      if i == 1:
        n_mcc_df = df[df.level == i][['mcc_id', 'mcc_name', 'account_id']]
        mcc_ids = n_mcc_df.merge(
          root_mcc_df, how='right', left_on='mcc_id', right_on='account_id'
        )[
          [
            'root_mcc_id',
            'root_mcc_name',
            'account_id_x',
            'mcc_id',
            'mcc_name',
            'account_id_y',
          ]
        ]
      else:
        previous_level = i - 1
        n_minus_1_mcc_df = df[df.level == previous_level][
          ['mcc_id', 'mcc_name', 'account_id']
        ]
        n_minus_1_mcc_df = n_minus_1_mcc_df.rename(
          columns={
            'mcc_id': f'level_{previous_level}_mcc_id',
            'mcc_name': f'level_{previous_level}_mcc_name',
          }
        )
        mcc_ids = n_mcc_df.merge(
          n_minus_1_mcc_df, how='right', left_on='mcc_id', right_on='account_id'
        )[
          [
            f'level_{previous_level}_mcc_id',
            f'level_{previous_level}_mcc_name',
            'account_id_x',
            'mcc_id',
            'mcc_name',
            'account_id_y',
          ]
        ]

      mcc_ids.mcc_name.fillna('Direct', inplace=True)
      mcc_ids.account_id_x.fillna(mcc_ids.account_id_y, inplace=True)
      mcc_ids.mcc_id.fillna(mcc_ids.account_id_y, inplace=True)
      mcc_ids = mcc_ids.rename(
        columns={
          'mcc_name': f'level_{i}_mcc_name',
          'account_id_x': 'account_id',
          'mcc_id': f'level_{i}_mcc_id',
        }
      )[
        selectable_columns
        + [f'level_{i}_mcc_id', f'level_{i}_mcc_name', 'account_id']
      ]
      mcc_ids[f'level_{i}_mcc_id'] = mcc_ids[f'level_{i}_mcc_id'].astype(int)
    mcc_ids['account_id'] = mcc_ids['account_id'].astype(int)
    return gaarf.report.GaarfReport.from_pandas(mcc_ids)


BUILTIN_QUERIES: dict[str, Callable] = {
  'ocid_mapping': get_ocid_mapping,
  'account_hierarchy_flattened': get_account_hierarchy_flattened,
}
