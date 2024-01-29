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
# limitations under the License.

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple
import logging
import abc
import os
import proto  # type: ignore
from dataclasses import dataclass
import datetime

from google.cloud import bigquery  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore
from pathlib import Path
import csv
import rich
from rich.console import Console
from rich.table import Table
import pandas as pd  # type: ignore
import numpy as np

from gaarf.report import GaarfReport
from gaarf.io import formatter

logger = logging.getLogger(__name__)


class AbsWriter(abc.ABC):

    def __init__(self,
                 array_handling: Literal["strings", "arrays"] = "strings",
                 array_separator: str = "|",
                 **kwargs) -> None:
        self.array_handling = array_handling
        self.array_separator = array_separator

    @abc.abstractmethod
    def write(self, report: GaarfReport, destination: str) -> str:
        pass

    def format_for_write(self, report) -> GaarfReport:
        array_handling_strategy = formatter.ArrayHandlingStrategy(
            type_=self.array_handling, delimiter=self.array_separator)
        return formatter.format_report_for_writing(report,
                                                   [array_handling_strategy])


class StdoutWriter(AbsWriter):

    def __init__(self, page_size: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.page_size = int(page_size)

    def write(self, report, destination):
        report = self.format_for_write(report)
        console = Console()
        table = Table(
            title=f"showing results for query <{destination.split('/')[-1]}>",
            caption=
            f"showing rows 1-{min(self.page_size, len(report))} out of total {len(report)}",
            box=rich.box.MARKDOWN)
        for header in report.column_names:
            table.add_column(header)
        for i, row in enumerate(report):
            if i < self.page_size:
                table.add_row(*[str(field) for field in row])
        console.print(table)


class CsvWriter(AbsWriter):

    def __init__(self,
                 destination_folder=os.getcwd(),
                 delimiter=",",
                 quotechar='"',
                 quoting=csv.QUOTE_MINIMAL,
                 **kwargs):
        super().__init__(**kwargs)
        self.destination_folder = destination_folder
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.quoting = quoting

    def __str__(self):
        return f"[CSV] - data are saved to {self.destination_folder} destination_folder."

    def write(self, report, destination) -> str:
        report = self.format_for_write(report)
        destination = formatter.format_extension(destination,
                                                 new_extension=".csv")
        if not os.path.isdir(self.destination_folder):
            os.makedirs(self.destination_folder)
        with open(os.path.join(self.destination_folder, destination),
                  "w") as file:
            writer = csv.writer(file,
                                delimiter=self.delimiter,
                                quotechar=self.quotechar,
                                quoting=self.quoting)
            writer.writerow(report.column_names)
            writer.writerows(report.results)
        return f"[CSV] - at {destination}"


class SheetWriter(AbsWriter):

    def __init__(self,
                 share_with: str,
                 credentials_file: str,
                 spreadsheet_url: Optional[str] = None,
                 is_append: bool = False,
                 **kwargs: str) -> None:
        """Initialize the SheetWriter to write reports to Google Sheets.

        Args:
            share_with: Email address to share the spreadsheet.
            credentials_file: Path to the service account credentials file.
            spreadsheet_url: URL of the Google Sheets spreadsheet.
            is_append: Whether you want to append data to the spreadsheet.
        """
        self.share_with = share_with
        self.credentials_file = credentials_file
        self.spreadsheet_url = spreadsheet_url
        self.is_append = is_append
        self.client = None
        self.spreadsheet = None

    def __str__(self):
        return f"[Sheet] - data are saved to {self.sheet_url}."

    def init_client(self) -> None:
        import gspread

        if not self.client:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            if not self.credentials_file:
                raise ValueError(
                    "Provide path to service account via `credentials_file` option"
                )
            self.gspread_client = gspread.service_account(
                filename=self.credentials_file)
            self._open_sheet()

    def _open_sheet(self) -> None:
        if not self.spreadsheet:
            if not self.spreadsheet_url:
                self.spreadsheet = self.gspread_client.create(
                    f'Gaarf CSV {datetime.datetime.utcnow()}')
            else:
                self.spreadsheet = self.gspread_client.open_by_url(
                    self.spreadsheet_url)

    def write(self,
              report,
              destination=f'Report {datetime.datetime.utcnow()}') -> str:
        import gspread
        report = self.format_for_write(report)
        self.init_client()
        if not destination:
            destination = f'Report {datetime.datetime.utcnow()}'
        destination = formatter.format_extension(destination)
        num_data_rows = len(report) + 1
        try:
            sheet = self.spreadsheet.worksheet(destination)
        except gspread.exceptions.WorksheetNotFound:
            sheet = self.spreadsheet.add_worksheet(destination,
                                                   rows=num_data_rows,
                                                   cols=len(
                                                       report.column_names))
        if not self.is_append:
            sheet.clear()
            self.add_rows_if_needed(num_data_rows, sheet)
            sheet.append_rows([report.column_names] + report.results,
                              value_input_option='RAW')
        else:
            self.add_rows_if_needed(num_data_rows, sheet)
            sheet.append_rows(report.results, value_input_option='RAW')

        success_msg = f"Report is saved to {sheet.url}"
        logger.info(success_msg)
        if self.share_with:
            self.spreadsheet.share(self.share_with,
                                   perm_type='user',
                                   role='writer')
        return success_msg

    def add_rows_if_needed(self, num_data_rows: int,
                           sheet: "gspread.worksheet.Worksheet") -> None:
        num_sheet_rows = len(sheet.get_all_values())
        if num_data_rows > num_sheet_rows:
            num_rows_to_add = num_data_rows - num_sheet_rows
            sheet.add_rows(num_rows_to_add)


class BigQueryWriter(AbsWriter):

    def __init__(self,
                 project: str,
                 dataset: str,
                 location: str = "US",
                 write_disposition: bigquery.WriteDisposition = bigquery.
                 WriteDisposition.WRITE_TRUNCATE,
                 **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.dataset_id = f"{project}.{dataset}"
        self.location = location
        self.write_disposition = write_disposition
        self.client = None

    def __str__(self):
        return f"[BigQuery] - {self.dataset_id} at {self.location} location."

    def create_or_get_dataset(self):
        self._init_client()
        try:
            bq_dataset = self.client.get_dataset(self.dataset_id)
        except NotFound:
            bq_dataset = bigquery.Dataset(self.dataset_id)
            bq_dataset.location = self.location
            bq_dataset = self.client.create_dataset(bq_dataset, timeout=30)
        return bq_dataset

    def write(self, report, destination) -> str:
        report = self.format_for_write(report)
        schema = self._define_schema(report)
        destination = formatter.format_extension(destination)
        table = self._create_or_get_table(f"{self.dataset_id}.{destination}",
                                          schema)
        job_config = bigquery.LoadJobConfig(
            write_disposition=self.write_disposition, schema=schema)

        if not report:
            df = pd.DataFrame(data=report.results_placeholder,
                              columns=report.column_names).head(0)
        else:
            df = report.from_pandas()
        df = df.replace({np.nan: None})
        logger.debug("Writing %d rows of data to %s", len(df), destination)
        try:
            self.client.load_table_from_dataframe(dataframe=df,
                                                  destination=table,
                                                  job_config=job_config)
            logger.debug("Writing to %s is completed", destination)
        except Exception as e:
            raise ValueError(f"Unable to save data to BigQuery! {str(e)}")
        return f"[BigQuery] - at {self.dataset_id}.{destination}"

    @staticmethod
    def _define_schema(report: GaarfReport) -> List[bigquery.SchemaField]:
        result_types = BigQueryWriter._get_result_types(report)
        return BigQueryWriter._get_bq_schema(result_types)

    @staticmethod
    def _get_result_types(report: GaarfReport) -> Dict[str, Dict[str, Any]]:
        result_types: Dict[str, Dict[str, Any]] = {}
        column_names = report.column_names
        for row in report.results or report.results_placeholder:
            if set(column_names) == set(result_types.keys()):
                break
            for i, field in enumerate(row):
                if field is None or column_names[i] in result_types:
                    continue
                field_type = type(field)
                if field_type in [
                        list,
                        proto.marshal.collections.repeated.RepeatedComposite,
                        proto.marshal.collections.repeated.Repeated
                ]:
                    repeated = True
                    if len(field) == 0:
                        field_type = str
                    else:
                        field_type = type(field[0])
                else:
                    field_type = type(field)
                    repeated = False
                result_types[column_names[i]] = {
                    "field_type": field_type,
                    "repeated": repeated
                }
        return result_types

    @staticmethod
    def _get_bq_schema(types) -> List[bigquery.SchemaField]:
        TYPE_MAPPING = {
            list: "REPEATED",
            str: "STRING",
            datetime.datetime: "DATETIME",
            datetime.date: "DATE",
            int: "INT64",
            float: "FLOAT64",
            bool: "BOOL",
            proto.marshal.collections.repeated.RepeatedComposite: "REPEATED",
            proto.marshal.collections.repeated.Repeated: "REPEATED"
        }

        schema: List[bigquery.SchemaField] = []
        for key, value in types.items():
            field_type = TYPE_MAPPING.get(value.get("field_type"))
            schema.append(
                bigquery.SchemaField(
                    name=key,
                    field_type="STRING" if not field_type else field_type,
                    mode="REPEATED" if value.get("repeated") else "NULLABLE"))
        return schema

    def _init_client(self) -> None:
        if not self.client:
            self.client = bigquery.Client(self.project)

    def _create_or_get_table(self, table_name: str, schema) -> bigquery.Table:
        self._init_client()
        try:
            table = self.client.get_table(table_name)
        except NotFound:
            table_ref = bigquery.Table(table_name, schema=schema)
            table = self.client.create_table(table_ref)
            table = self.client.get_table(table_name)
        return table


class SqlAlchemyWriter(AbsWriter):

    def __init__(self,
                 connection_string: str,
                 if_exists: str = "replace",
                 **kwargs):
        super().__init__(**kwargs)
        self.connection_string = connection_string
        self.if_exists = if_exists

    def write(self, report, destination):
        report = self.format_for_write(report)
        destination = formatter.format_extension(destination)
        if not report:
            df = pd.DataFrame(data=report.results_placeholder,
                              columns=report.column_names).head(0)
        else:
            df = report.from_pandas()
        logger.debug("Writing %d rows of data to %s", len(df), destination)
        df.to_sql(name=destination,
                  con=self._create_engine(),
                  if_exists=self.if_exists)
        logger.debug("Writing to %s is completed", destination)

    def _create_engine(self):
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)


class NullWriter(AbsWriter):

    def __init__(self, writer_option, **kwargs):
        raise ValueError(f"{writer_option} is unknown writer type!")

    def write(self):
        print("Unknown writer type!")


class WriterFactory:
    write_options: Dict[str, AbsWriter] = {}

    def __init__(self):
        self.load_writer_options()

    def load_writer_options(self):
        self.write_options["bq"] = BigQueryWriter
        self.write_options["csv"] = CsvWriter
        self.write_options["sheet"] = SheetWriter
        self.write_options["console"] = StdoutWriter
        self.write_options["sqldb"] = SqlAlchemyWriter

    def create_writer(self, writer_option, **kwargs):
        if writer_option in self.write_options:
            return self.write_options[writer_option](**kwargs)
        else:
            return NullWriter(writer_option)


class ZeroRowException(Exception):
    pass
