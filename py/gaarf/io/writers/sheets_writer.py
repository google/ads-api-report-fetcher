# Copyright 2024 Google LLC
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
# limitations under the License.
"""Module for writing data to Google Sheets."""

from __future__ import annotations

from google.auth import exceptions as auth_exceptions

try:
  import gspread
except ImportError as e:
  raise ImportError(
    'Please install google-ads-api-report-fetcher with sheets support - '
    '`pip install google-ads-api-report-fetcher[sheet]`'
  ) from e

import datetime
import logging

from gaarf.io import formatter
from gaarf.io.writers.abs_writer import AbsWriter
from gaarf.report import GaarfReport


class SheetWriter(AbsWriter):
  def __init__(
    self,
    share_with: str,
    credentials_file: str,
    spreadsheet_url: str | None = None,
    is_append: bool = False,
    **kwargs: str,
  ) -> None:
    """Initialize the SheetWriter to write reports to Google Sheets.

    Args:
        share_with: Email address to share the spreadsheet.
        credentials_file: Path to the service account credentials file.
        spreadsheet_url: URL of the Google Sheets spreadsheet.
        is_append: Whether you want to append data to the spreadsheet.
    """
    super().__init__(**kwargs)
    self.share_with = share_with
    self.credentials_file = credentials_file
    self.spreadsheet_url = spreadsheet_url
    self.is_append = is_append
    self.client = None
    self.spreadsheet = None
    self.gspread_client = None

  def write(
    self,
    report: GaarfReport,
    destination: str = f'Report {datetime.datetime.utcnow()}',
  ) -> str:
    self._init_client()
    report = self.format_for_write(report)
    if not destination:
      destination = f'Report {datetime.datetime.utcnow()}'
    destination = formatter.format_extension(destination)
    num_data_rows = len(report) + 1
    try:
      sheet = self.spreadsheet.worksheet(destination)
    except gspread.exceptions.WorksheetNotFound:
      sheet = self.spreadsheet.add_worksheet(
        destination, rows=num_data_rows, cols=len(report.column_names)
      )
    if not self.is_append:
      sheet.clear()
      self._add_rows_if_needed(num_data_rows, sheet)
      sheet.append_rows(
        [report.column_names] + report.results, value_input_option='RAW'
      )
    else:
      self._add_rows_if_needed(num_data_rows, sheet)
      sheet.append_rows(report.results, value_input_option='RAW')

    success_msg = f'Report is saved to {sheet.url}'
    logging.info(success_msg)
    if self.share_with:
      self.spreadsheet.share(self.share_with, perm_type='user', role='writer')
    return success_msg

  def _init_client(self) -> None:
    if not self.client:
      if not self.credentials_file:
        raise ValueError(
          'Provide path to service account via ' '`credentials_file` option'
        )
      try:
        self.gspread_client = gspread.service_account(
          filename=self.credentials_file
        )
      except auth_exceptions.MalformedError:
        self.gspread_client = gspread.oauth(
          credentials_filename=self.credentials_file
        )
      self._open_sheet()

  def _open_sheet(self) -> None:
    if not self.spreadsheet:
      if not self.spreadsheet_url:
        self.spreadsheet = self.gspread_client.create(
          f'Gaarf CSV {datetime.datetime.utcnow()}'
        )
      else:
        self.spreadsheet = self.gspread_client.open_by_url(self.spreadsheet_url)

  def _add_rows_if_needed(
    self, num_data_rows: int, sheet: gspread.worksheet.Worksheet
  ) -> None:
    num_sheet_rows = len(sheet.get_all_values())
    if num_data_rows > num_sheet_rows:
      num_rows_to_add = num_data_rows - num_sheet_rows
      sheet.add_rows(num_rows_to_add)

  def __str__(self) -> str:
    return f'[Sheet] - data are saved to {self.spreadsheet_url}.'
