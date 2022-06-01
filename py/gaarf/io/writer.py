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
# limitations under the License.import csv

from typing import Any, Dict, Sequence
import abc
import os
import proto  # type: ignore
from google.cloud import bigquery  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore
from pathlib import Path
import csv
import pandas as pd  # type: ignore
from tabulate import tabulate
from .formatter import BigQueryFormatter, ResultsFormatter  # type: ignore


class AbsWriter(abc.ABC):
    @abc.abstractmethod
    def write(self, results: Sequence[Any], destination: str,
              header: Sequence[str]) -> str:
        pass

    @abc.abstractmethod
    def _define_header(self, results, header):
        pass


class StdoutWriter(AbsWriter):
    def __init__(self, page_size=10, **kwargs):
        self.page_size = page_size

    def write(self, results, destination, header):
        results = ResultsFormatter.format(results)
        results_generator = self._paginate_rows(results, self.page_size)
        message = f"showing results for query {destination}"
        print("=" * len(message))
        print(message)
        print("=" * len(message))
        print(tabulate(next(results_generator), headers=header))

    def _define_header(self, results, header):
        pass

    def _paginate_rows(self, results, page_size):
        for i in range(0, len(results), page_size):
            print(f"showing rows {i+1}-{i+page_size} out of total "
                  f"{len(results)} rows")
            yield results[i:(i + page_size)]


class CsvWriter(AbsWriter):
    def __init__(self,
                 destination_folder=os.getcwd(),
                 delimiter=",",
                 quotechar='"',
                 quoting=csv.QUOTE_MINIMAL,
                 **kwargs):
        self.destination_folder = destination_folder
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.quoting = quoting

    def __str__(self):
        return f"[CSV] - data are saved to {self.destination_folder} destination_folder."

    def write(self, results, destination, header) -> str:
        results = ResultsFormatter.format(results)
        header = self._define_header(results, header)
        destination = DestinationFormatter.format_extension(
            destination, new_extension=".csv")
        with open(os.path.join(self.destination_folder, destination),
                  "w") as file:
            writer = csv.writer(file,
                                delimiter=self.delimiter,
                                quotechar=self.quotechar,
                                quoting=self.quoting)
            writer.writerow(header)
            writer.writerows(results)
        return f"[CSV] - at {destination}"

    def _define_header(self, results, header):
        return header


class BigQueryWriter(AbsWriter):
    def __init__(self,
                 project: str,
                 dataset: str,
                 location: str = "US",
                 **kwargs):
        self.client = bigquery.Client()
        self.dataset_id = f"{project}.{dataset}"
        self.location = location

    def __str__(self):
        return f"[BigQuery] - {self.dataset_id} at {self.location} location."

    def write(self, results, destination, header) -> str:
        results = ResultsFormatter.format(results)
        formatted_results = BigQueryFormatter.format(results, "|")
        schema = self._define_header(formatted_results, header)
        self._create_or_get_dataset()
        destination = DestinationFormatter.format_extension(destination)
        table = self._create_or_get_table(f"{self.dataset_id}.{destination}",
                                          schema)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema)

        df = pd.DataFrame(formatted_results, columns=header)
        try:
            self.client.load_table_from_dataframe(dataframe=df,
                                                  destination=table,
                                                  job_config=job_config)
        except Exception as e:
            raise ValueError(f"Unable to save data to BigQuery! {str(e)}")
        return f"[BigQuery] - at {self.dataset_id}.{destination}"

    def _define_header(self, results, header):
        result_types = self._get_result_types(results, header)
        return self._get_bq_schema(result_types)

    def _get_result_types(
            self, elements: Sequence[Any],
            column_names: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        result_types = {}
        for i, element in enumerate(elements[0]):
            element_type = type(element)
            if element_type in [
                    list, proto.marshal.collections.repeated.RepeatedComposite,
                    proto.marshal.collections.repeated.Repeated
            ]:
                repeated = True
                if len(element) == 0:
                    element_type = str
                else:
                    element_type = type(element[0])
            else:
                element_type = type(element)
                repeated = False
            result_types[column_names[i]] = {
                "element_type": element_type,
                "repeated": repeated
            }
        return result_types

    def _get_bq_schema(self, types):
        TYPE_MAPPING = {
            list: "REPEATED",
            str: "STRING",
            int: "INT64",
            float: "FLOAT64",
            bool: "BOOL",
            proto.marshal.collections.repeated.RepeatedComposite: "REPEATED",
            proto.marshal.collections.repeated.Repeated: "REPEATED"
        }

        schema = []
        for key, value in types.items():
            element_type = TYPE_MAPPING.get(value.get("element_type"))
            schema.append(
                bigquery.SchemaField(
                    name=key,
                    field_type="STRING" if not element_type else element_type,
                    mode="REPEATED" if value.get("repeated") else "NULLABLE"))
        return schema

    def _create_or_get_dataset(self):
        try:
            bq_dataset = self.client.get_dataset(self.dataset_id)
        except NotFound:
            bq_dataset = bigquery.Dataset(self.dataset_id)
            bq_dataset.location = self.location
            bq_dataset = self.client.create_dataset(bq_dataset, timeout=30)
        return bq_dataset

    def _create_or_get_table(self, table_name, schema):
        try:
            table = self.client.get_table(table_name)
        except NotFound:
            table_ref = bigquery.Table(table_name, schema=schema)
            table = self.client.create_table(table_ref)
            table = self.client.get_table(table_name)
        return table


class NullWriter(AbsWriter):
    def __init__(self, writer_option, **kwargs):
        raise ValueError(f"{writer_option} is unknown writer type!")

    def write(self):
        print("Unknown writer type!")

    def _define_header(self):
        pass


class WriterFactory:
    write_options: Dict[str, AbsWriter] = {}

    def __init__(self):
        self.load_writer_options()

    def load_writer_options(self):
        self.write_options["bq"] = BigQueryWriter
        self.write_options["csv"] = CsvWriter
        self.write_options["console"] = StdoutWriter

    def create_writer(self, writer_option, **kwargs):
        if writer_option in self.write_options:
            return self.write_options[writer_option](**kwargs)
        else:
            return NullWriter(writer_option)


class DestinationFormatter:
    @staticmethod
    def format_extension(path_object: str,
                         current_extension: str = ".sql",
                         new_extension: str = "") -> str:
        return Path(path_object).name.replace(current_extension, new_extension)


class ZeroRowException(Exception):
    pass
