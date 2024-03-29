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
from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from operator import attrgetter

from dateutil.relativedelta import relativedelta
from gaarf.api_clients import BaseClient
from gaarf.api_clients import GOOGLE_ADS_API_VERSION
from gaarf.query_post_processor import PostProcessorMixin


@dataclass(frozen=True)
class VirtualColumn:
    type: str
    value: str
    fields: list[str] | None = None
    substitute_expression: str | None = None


@dataclass
class ExtractedLineElements:
    fields: list[str] | None
    alias: str | None
    virtual_column: VirtualColumn | None


@dataclass
class ProcessedField:
    field: str
    customizer_type: str | None = None
    customizer_value: str | None = None


class VirtualColumnError(Exception):
    ...


class FieldError(Exception):
    ...


class MacroError(Exception):
    ...


@dataclass
class QueryElements:
    """Contains raw query and parsed elements.

    Attributes:
        query_title: Title of the query that needs to be parsed.
        query_text: Text of the query that needs to be parsed.
        resource_name: Name of Google Ads API reporting resource.
        fields: Ads API fields that need to be feched.
        column_names: Friendly names for fields which are used when saving data
        customizers: Attributes of fields that need to be be extracted.
        virtual_columns: Attributes of fields that need to be be calculated.
        is_constant_resource: Whether resource considered a constant one.
        is_builtin_query: Whether query is built-in.
    """
    query_title: str
    query_text: str
    resource_name: str
    fields: list[str] | None = None
    column_names: list[str] | None = None
    customizers: dict[str, dict[str, str]] | None = None
    virtual_columns: dict[str, VirtualColumn] | None = None
    is_constant_resource: bool = False
    is_builtin_query: bool = False


class CommonParametersMixin:
    common_params = {
        'date_iso':
        datetime.date.today().strftime('%Y%m%d'),
        'yesterday_iso':
        (datetime.date.today() - relativedelta(days=1)).strftime('%Y%m%d'),
        'current_date':
        datetime.date.today().strftime('%Y-%m-%d'),
        'current_datetime':
        datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    }


class QuerySpecification(CommonParametersMixin, PostProcessorMixin):

    def __init__(self,
                 text: str,
                 title: str | None = None,
                 args: dict | None = None,
                 api_version: str = GOOGLE_ADS_API_VERSION) -> None:
        self.text = text
        self.title = title
        self.args = args or {}
        self.macros = self._init_macros()
        self.base_client = BaseClient(api_version)

    def _init_macros(self) -> dict[str, str]:
        if not self.args:
            return self.common_params
        if macros := self.args.get('macro'):
            macros.update(self.common_params)
            return macros
        return self.common_params

    def generate(self) -> QueryElements:
        """Generates necessary query elements based on query text and arguments.

        Returns:
            Various elements parsed from a query (text, fields,
            column_names, etc).
        """

        query_text = self.expand_jinja(self.text, self.args.get('template'))
        resource_name = self.extract_resource_from_query(query_text)
        if is_builtin_query := bool(resource_name.startswith('builtin')):
            return QueryElements(query_title=resource_name.replace(
                'builtin.', ''),
                                 query_text=query_text,
                                 fields=None,
                                 column_names=None,
                                 customizers=None,
                                 virtual_columns=None,
                                 resource_name=resource_name,
                                 is_constant_resource=False,
                                 is_builtin_query=True)
        if not is_builtin_query and resource_name not in dir(
                self.base_client.google_ads_row):
            raise ValueError(
                f'Invalid resource specified in the query: {resource_name}')
        is_constant_resource = bool(resource_name.endswith('_constant'))
        query_lines = self.cleanup_query_text(query_text)
        query_text = ' '.join(query_lines)
        try:
            query_text = query_text.format(**self.macros)
        except KeyError as e:
            raise MacroError(f'No value provided for macro {str(e)}.') from e
        fields = []
        column_names = []
        customizers = {}
        virtual_columns = {}

        query_lines = self.extract_query_lines(' '.join(query_lines))
        for line in query_lines:
            if not line:
                continue
            field_name = None
            customizer_type = None
            line_elements = self._extract_line_elements(line, self.macros)
            if field_elements := line_elements.fields:
                for field_element in field_elements:
                    processed_field = self._process_field(field_element)
                    field_name = processed_field.field.strip().replace(',', '')
                    fields.append(self.format_type_field_name(field_name))
                    customizer_type = processed_field.customizer_type
            if not line_elements.alias and not field_name and not is_builtin_query:
                raise VirtualColumnError(
                    'Virtual attributes should be aliased')
            column_name = line_elements.alias.strip().replace(
                ',', '') if line_elements.alias else field_name
            if column_name:
                column_names.append(self.normalize_column_name(column_name))

            if virtual_column := line_elements.virtual_column:
                virtual_columns[column_name] = virtual_column
            if customizer_type:
                customizers[column_name] = {
                    'type': customizer_type,
                    'value': processed_field.customizer_value
                }
        query_text = self.create_query_text(fields, virtual_columns,
                                            query_text)
        for _, column in virtual_columns.items():
            if not isinstance(column.value, (int, float)):
                column.value.format(**self.macros)
        return QueryElements(query_title=self.title,
                             query_text=query_text,
                             fields=fields,
                             column_names=column_names,
                             customizers=customizers,
                             virtual_columns=virtual_columns,
                             resource_name=resource_name,
                             is_constant_resource=is_constant_resource,
                             is_builtin_query=is_builtin_query)

    def create_query_text(self, fields: list[str],
                          virtual_columns: dict[str, VirtualColumn],
                          query_text: str) -> str:
        """Generate valid GAQL query.

        Based on original Gaarf query text, a set of field and virtual columns
        constructs new GAQL query to be sent to Ads API.

        Args:
            fields:
                All fields that need to be fetched from API.
            virtual_columns:
                Virtual columns that might contain extra fields for fetching.
            query_text: Original Gaarf query text.
        Returns:
            Description of return.
        """
        virtual_fields = [
            field for name, column in virtual_columns.items()
            if column.type == 'expression' for field in column.fields
        ]
        if virtual_fields:
            fields = fields + virtual_fields
        query_text = (f"SELECT {', '.join(fields)} "
                      f'{self.extract_from_statement(query_text)}')
        query_text = self._remove_traling_comma(query_text)
        query_text = self._unformat_type_field_name(query_text)
        query_text = re.sub(r'\s+', ' ', query_text).strip()
        return query_text

    def cleanup_query_text(self, query_text: str) -> list[str]:
        """Removes comments and converts text to lines."""
        query_lines = query_text.split('\n')
        result: list[str] = []
        for line in query_lines:
            if re.match('^(#|--|//)', line):
                continue
            cleaned_query_line = re.sub(';$', '',
                                        re.sub('(--|//).*$', '', line).strip())
            result.append(cleaned_query_line)
        return result

    def extract_resource_from_query(self, query: str) -> str:
        return str(
            re.findall(r'FROM\s+([\w.]+)', query,
                       flags=re.IGNORECASE)[0]).strip()

    def extract_query_lines(self, query_text: str) -> list[str]:
        selected_fields = re.sub(r'\bSELECT\b|FROM .*',
                                 '',
                                 query_text,
                                 flags=re.IGNORECASE).split(',')
        return [field.strip() for field in selected_fields if field != ' ']

    def extract_from_statement(self, query_text: str) -> str:
        return re.search(' FROM .+', query_text, re.IGNORECASE).group(0)

    def _extract_line_elements(self, query_line: str,
                               macros: dict) -> ExtractedLineElements:
        fields: list[str] = []
        virtual_column = None
        field_raw, *alias = re.split(' [Aa][Ss] ', query_line)
        field_raw = field_raw.replace(r'\s+', '').strip()
        processed_field = self._process_field(field_raw)
        virtual_field = processed_field.field
        try:
            _ = attrgetter(virtual_field)(self.base_client.google_ads_row)
        except AttributeError:
            if virtual_field.isdigit():
                virtual_field = int(virtual_field)
            else:
                try:
                    virtual_field = float(virtual_field)
                except ValueError:
                    pass

            operators = ('/', r'\*', r'\+', ' - ')
            if len(expressions := re.split('|'.join(operators),
                                           field_raw)) > 1:
                virtual_column_fields = []
                substitute_expression = virtual_field
                for element in expressions:
                    element = element.strip()
                    try:
                        _ = attrgetter(element)(
                            self.base_client.google_ads_row)
                        virtual_column_fields.append(element)
                        substitute_expression = substitute_expression.replace(
                            element, f'{{{element}}}')
                    except AttributeError:
                        pass
                virtual_column = VirtualColumn(
                    type='expression',
                    value=virtual_field.format(
                        **macros) if macros else virtual_field,
                    fields=virtual_column_fields,
                    substitute_expression=substitute_expression.replace(
                        '.', '_'))
            else:
                if not isinstance(virtual_field, (int, float)):
                    if not self._not_a_quoted_string(virtual_field):
                        raise FieldError(
                            f"Incorrect field '{virtual_field}' in the query '{self.text}'."
                        )
                    virtual_field = virtual_field.replace("'",
                                                          '').replace('"', '')
                    virtual_field = virtual_field.format(
                        **macros) if macros else virtual_field
                virtual_column = VirtualColumn(type='built-in',
                                               value=virtual_field)
        if not virtual_column and field_raw:
            fields = [field_raw]
        else:
            fields = None
        return ExtractedLineElements(fields=fields,
                                     alias=alias[0] if alias else None,
                                     virtual_column=virtual_column)

    def _process_field(self, raw_field: str) -> ProcessedField:
        """Process field to extract possible customizers.

        Args:
            field: Unformatted field string value.
        Returns:
            ProcessedField that contains formatted field with customizers.
        """
        if len(resources := self.extract_resource_element(raw_field)) > 1:
            field_name, resource_index = resources
            return ProcessedField(field=field_name,
                                  customizer_type='resource_index',
                                  customizer_value=int(resource_index))

        if len(nested_fields := self.extract_nested_resource(raw_field)) > 1:
            field_name, nested_field = nested_fields
            return ProcessedField(field=field_name,
                                  customizer_type='nested_field',
                                  customizer_value=nested_field)
        if len(pointers := self.extract_pointer(raw_field)) > 1:
            field_name, pointer = pointers
            return ProcessedField(field=field_name,
                                  customizer_type='pointer',
                                  customizer_value=pointer)
        return ProcessedField(field=raw_field)

    def extract_resource_element(self, line_elements: str) -> list[str]:
        return re.split('~', line_elements)

    def extract_pointer(self, line_elements: str) -> list[str]:
        return re.split('->', line_elements)

    def extract_nested_resource(self, line_elements: str) -> list[str]:
        return re.split(':', line_elements)

    def format_type_field_name(self, field_name: str) -> str:
        return re.sub(r'\.type', '.type_', field_name)

    def normalize_column_name(self, column_name: str) -> str:
        return re.sub(r'\.', '_', column_name)

    def _remove_traling_comma(self, query: str) -> str:
        return re.sub(r',\s+from', ' FROM', query, re.IGNORECASE)

    def _unformat_type_field_name(self, query: str) -> str:
        return re.sub(r'\.type_', '.type', query)

    def _not_a_quoted_string(self, field_name: str) -> bool:
        if ((field_name.startswith("'") and field_name.endswith("'"))
                or (field_name.startswith('"') and field_name.endswith('"'))):
            return True
        return False
