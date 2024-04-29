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
"""Module for various helpers for executing Gaarf as CLI tool."""
from __future__ import annotations

import argparse
import dataclasses
import datetime
import logging
import os
import sys
import traceback
from collections.abc import MutableSequence
from collections.abc import Sequence
from typing import Any
from typing import Callable

import rich
import smart_open
import yaml
from dateutil import relativedelta
from gaarf import query_editor
from gaarf.io import writer
from google.ads.googleads import errors as google_ads_errors


@dataclasses.dataclass
class BaseConfig:
    """Base config to inherit other configs from."""


@dataclasses.dataclass
class GaarfConfig(BaseConfig):
    """Stores values to run gaarf from command line.

    Attributes:
        output:
            Specifies where to store fetched data (console, csv, BQ.)
        api_version:
            Google Ads API version.
        account:
            Account(s) to get data from.
        params:
            Any parameters passed to Gaarf query for substitution.
        writer_params:
            Any parameters that can be passed to writer for data saving.
        customer_ids_query:
            Query text to limit accounts fetched from Ads API.
        customer_ids_query_file:
            Path to query to limit accounts fetched from Ads API.
    """
    output: str
    api_version: str
    account: str | list[str] | None
    params: dict[str, Any] = dataclasses.field(default_factory=dict)
    writer_params: dict[str, Any] = dataclasses.field(default_factory=dict)
    customer_ids_query: str | None = None
    customer_ids_query_file: str | None = None

    def __post_init__(self) -> None:
        """Ensures that values passed during __init__ correctly formatted."""
        if isinstance(self.account, MutableSequence):
            self.account = [
                account.replace('-', '').strip() for account in self.account
            ]
        else:
            self.account = self.account.replace(
                '-', '').strip() if self.account else None
        self.writer_params = {
            key.replace('-', '_'): value
            for key, value in self.writer_params.items()
        }


class GaarfConfigException(Exception):
    """Exception for invalid GaarfConfig."""


@dataclasses.dataclass
class GaarfBqConfig(BaseConfig):
    """Stores values to run gaarf-bq from command line.

    Attributes:
        project:
           Google Cloud project name.
        dataset_location:
            Location of BigQuery dataset.
        params:
            Any parameters passed to BigQuery query for substitution.
    """
    project: str
    dataset_location: str | None
    params: dict[str, Any] = dataclasses.field(default_factory=dict)


class GaarfBqConfigException(Exception):
    """Exception for invalid GaarfBqConfig."""


@dataclasses.dataclass
class GaarfSqlConfig(BaseConfig):
    """Stores values to run gaarf-sql from command line.

    Attributes:
        connection_string:
           Connection string to SqlAlchemy database engine.
        params:
            Any parameters passed to SQL query for substitution.
    """
    connection_string: str
    params: dict[str, Any] = dataclasses.field(default_factory=dict)


class GaarfSqlConfigException(Exception):
    """Exception for invalid GaarfSqlConfig."""


class BaseConfigBuilder:
    """Base class for building CLI-based Gaarf configs.

    Attributes:
        args: Parameters to initialize config.
        type: Type of config (either 'file' or 'cli').
        gaarf_config_path: Path to config file.
    """

    def __init__(self, args: tuple[argparse.Namespace, list]) -> None:
        """Initializes config based on provided arguments.

        Args:
            args: Parameters to initialize config.
        """
        self.args = args
        if (gaarf_config_path := self.args[0].gaarf_config):
            self.type = 'file'
            self.gaarf_config_path = gaarf_config_path
        else:
            self.type = 'cli'
            self.gaarf_config_path = None

    def build(self) -> BaseConfig:
        """Loads config from file or build from arguments.

        Returns:
            A subclass of BaseConfig.
        """
        if self.type == 'file':
            return self._load_gaarf_config()
        return self._build_gaarf_config()

    def _load_gaarf_config(self) -> BaseConfig:
        """Loads config from gaarf_config_path."""
        raise NotImplementedError

    def _build_gaarf_config(self) -> BaseConfig:
        """Builds config from instance args."""
        raise NotImplementedError


class GaarfConfigBuilder(BaseConfigBuilder):
    """Builds GaarfConfig from CLI.

    Attributes:
        args: Parameters to initialize config.
        type: Type of config (either 'file' or 'cli').
        gaarf_config_path: Path to config file.
    """

    def _load_gaarf_config(self) -> GaarfConfig:
        """Loads config from gaarf_config_path.

        Returns:
            GaarfConfig with values taken from config file.

        Raises:
            GaarfConfigException:
                If config file missing `gaarf` section.
            ValueError:
                If config file missing `output` section.
        """
        with smart_open.open(self.gaarf_config_path, encoding='utf-8') as f:
            config = yaml.safe_load(f)
        gaarf_section = config.get('gaarf')
        if not gaarf_section:
            raise GaarfConfigException(
                'Invalid config, must have `gaarf` section!')
        if not (output := gaarf_section.get('output')):
            raise ValueError('Config does not contains `output` section!')
        return GaarfConfig(
            output=gaarf_section.get('output'),
            api_version=gaarf_section.get('api_version'),
            account=gaarf_section.get('account'),
            params=gaarf_section.get('params', {}),
            writer_params=gaarf_section.get(output),
            customer_ids_query=gaarf_section.get('customer_ids_query'),
            customer_ids_query_file=gaarf_section.get(
                'customer_ids_query_file'))

    def _build_gaarf_config(self) -> GaarfConfig:
        """Builds config from args parameter.

        Returns:
            GaarfConfig with values taken from instance args.
        """
        main_args, query_args = self.args[0], self.args[1]
        params = ParamsParser([
            'macro',
            'template',
            main_args.save,
        ]).parse(query_args)
        return GaarfConfig(
            output=main_args.save,
            api_version=main_args.api_version,
            account=main_args.customer_id,
            params=params,
            writer_params=params.get(main_args.save),
            customer_ids_query=main_args.customer_ids_query,
            customer_ids_query_file=main_args.customer_ids_query_file)


class GaarfBqConfigBuilder(BaseConfigBuilder):
    """Builds GaarfBqConfig from CLI.

    Attributes:
        args: Parameters to initialize config.
        type: Type of config (either 'file' or 'cli').
        gaarf_config_path: Path to config file.
    """

    def _load_gaarf_config(self) -> GaarfBqConfig:
        """Loads config from gaarf_config_path.

        Returns:
            GaarfBqConfig with values taken from config file.

        Raises:
            GaarfBqConfigException:
                If config file missing `gaarf-bq` section.
        """
        with smart_open.open(self.gaarf_config_path, encoding='utf-8') as f:
            config = yaml.safe_load(f)
        gaarf_section = config.get('gaarf-bq')
        if not gaarf_section:
            raise GaarfBqConfigException(
                'Invalid config, must have `gaarf-bq` section!')
        params = gaarf_section.get('params', {})
        return GaarfBqConfig(
            project=gaarf_section.get('project'),
            dataset_location=gaarf_section.get('dataset_location'),
            params=params)

    def _build_gaarf_config(self) -> GaarfBqConfig:
        """Builds config from args parameter.

        Returns:
            GaarfBqConfig with values taken from instance args.
        """
        main_args, query_args = self.args[0], self.args[1]
        params = ParamsParser([
            'macro',
            'sql',
            'template',
        ]).parse(query_args)
        return GaarfBqConfig(
            project=main_args.project,
            dataset_location=main_args.dataset_location,
            params=params)


class GaarfSqlConfigBuilder(BaseConfigBuilder):
    """Builds GaarfSqlConfig from CLI.

    Attributes:
        args: Parameters to initialize config.
        type: Type of config (either 'file' or 'cli').
        gaarf_config_path: Path to config file.
    """

    def _load_gaarf_config(self) -> BaseConfig:
        """Loads config from gaarf_config_path.

        Returns:
            GaarfBqConfig with values taken from config file.

        Raises:
            GaarfSqlConfigException:
                If config file missing `gaarf-sql` section.
        """
        with smart_open.open(self.gaarf_config_path, encoding='utf-8') as f:
            config = yaml.safe_load(f)
        gaarf_section = config.get('gaarf-sql')
        if not gaarf_section:
            raise GaarfSqlConfigException(
                'Invalid config, must have `gaarf-sql` section!')
        params = gaarf_section.get('params', {})
        return GaarfSqlConfig(
            connection_string=gaarf_section.get('connection-string').format(
                **dict(os.environ.items())),
            params=params)

    def _build_gaarf_config(self) -> GaarfSqlConfig:
        """Builds config from args parameter.

        Returns:
            GaarfSqlConfig with values taken from instance args.
        """
        main_args, query_args = self.args[0], self.args[1]
        params = ParamsParser(['macro', 'sql', 'template']).parse(query_args)
        return GaarfSqlConfig(
            connection_string=main_args.connection_string.format(
                **dict(os.environ.items())),
            params=params)


class ParamsParser(query_editor.CommonParametersMixin):

    def __init__(self, identifiers: Sequence[str]) -> None:
        self.identifiers = identifiers

    def parse(self, params: Sequence) -> dict[str, dict | None]:
        return {
            identifier: self._parse_params(identifier, params)
            for identifier in self.identifiers
        }

    def _parse_params(self, identifier: str, params: Sequence[Any]) -> dict:
        parsed_params = {}
        if params:
            raw_params = [param.split('=', maxsplit=1) for param in params]
            for param in raw_params:
                param_pair = self._identify_param_pair(identifier, param)
                if param_pair:
                    parsed_params.update(param_pair)
        return parsed_params

    def _identify_param_pair(self, identifier: str,
                             param: Sequence[str]) -> dict[str, Any] | None:
        key = param[0]
        if identifier not in key:
            return None
        provided_identifier, key = key.split('.')
        if provided_identifier.replace('--', '') not in self.identifiers:
            raise GaarfParamsException(
                f'CLI argument {provided_identifier} is not supported'
                f", supported arguments {', '.join(self.identifiers)}")
        key = key.replace('-', '_')
        if len(param) == 2:
            # TODO: b/337860595 - Ensure that writer params are converted to int
            return {key: param[1]}
        raise GaarfParamsException(
            f'{identifier} {key} is invalid,'
            f'--{identifier}.key=value is the correct format')


class GaarfParamsException(Exception):
    """Defines exception for incorrect parameters."""


def convert_date(date_string: str) -> str:
    """Converts specific dates parameters to actual dates.

    Returns:
        Date string in YYYY-MM-DD format.

    Raises:
        ValueError:
            If dynamic lookback value (:YYYYMMDD-N) is incorrect.
    """

    if isinstance(date_string, list) or date_string.find(':YYYY') == -1:
        return date_string
    current_date = datetime.date.today()
    date_object = date_string.split('-')
    base_date = date_object[0]
    if len(date_object) == 2:
        try:
            days_ago = int(date_object[1])
        except ValueError as e:
            raise ValueError(
                'Must provide numeric value for a number lookback period, '
                'i.e. :YYYYMMDD-1') from e
    else:
        days_ago = 0
    if base_date == ':YYYY':
        new_date = datetime.datetime(current_date.year, 1, 1)
        delta = relativedelta.relativedelta(years=days_ago)
    elif base_date == ':YYYYMM':
        new_date = datetime.datetime(current_date.year, current_date.month, 1)
        delta = relativedelta.relativedelta(months=days_ago)
    elif base_date == ':YYYYMMDD':
        new_date = current_date
        delta = relativedelta.relativedelta(days=days_ago)
    return (new_date - delta).strftime('%Y-%m-%d')


class ConfigSaver:

    def __init__(self, path: str) -> None:
        self.path = path

    def save(self, gaarf_config: BaseConfig):
        if os.path.exists(self.path):
            with smart_open.open(self.path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        else:
            config = {}
        config = self.prepare_config(config, gaarf_config)
        with smart_open.open(self.path, 'w', encoding='utf-8') as f:
            yaml.dump(
                config,
                f,
                default_flow_style=False,
                sort_keys=False,
                encoding='utf-8')

    def prepare_config(self, config: dict, gaarf_config: BaseConfig) -> dict:
        gaarf = dataclasses.asdict(gaarf_config)
        if isinstance(gaarf_config, GaarfConfig):
            gaarf[gaarf_config.output] = gaarf_config.writer_params
            if not isinstance(gaarf_config.account, MutableSequence):
                gaarf['account'] = gaarf_config.account.split(',')
            del gaarf['writer_params']
            if gaarf_config.writer_params:
                del gaarf['params'][gaarf_config.output]
            gaarf = _remove_empty_values(gaarf)
            config.update({'gaarf': gaarf})
        if isinstance(gaarf_config, GaarfBqConfig):
            gaarf = _remove_empty_values(gaarf)
            config.update({'gaarf-bq': gaarf})
        if isinstance(gaarf_config, GaarfSqlConfig):
            gaarf = _remove_empty_values(gaarf)
            config.update({'gaarf-sql': gaarf})
        return config


def initialize_runtime_parameters(config: BaseConfig) -> BaseConfig:
    for key, param in config.params.items():
        for key_param, value_param in param.items():
            config.params[key][key_param] = convert_date(value_param)
        config.params[key].update(ParamsParser.common_params)
    return config


def _remove_empty_values(dict_object: dict[str, Any]) -> dict[str, Any]:
    """Remove all empty elements: strings, dictionaries from a dictionary."""
    if isinstance(dict_object, dict):
        return {
            key: value
            for key, value in ((key, _remove_empty_values(value))
                               for key, value in dict_object.items())
            if value
        }
    if isinstance(dict_object, (int, str, MutableSequence)):
        return dict_object


def gaarf_runner(query: str, callback: Callable, logger) -> None:
    try:
        logger.debug('starting query %s', query)
        callback()
        logger.info('%s executed successfully', query)
    except writer.ZeroRowException:
        logger.warning('%s returns 0 rows', query)
    except google_ads_errors.GoogleAdsException as ex:
        logger.error(
            '%s failed with status %s and includes the following errors:',
            query,
            ex.error.code().name)
        for error in ex.failure.errors:
            logger.error('\tError with message %s .', error.message)
            if error.location:
                for field in error.location.field_path_elements:
                    logger.error('\t\tOn field %s', field.field_name)
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error('%s generated an exception: %s', query, str(e))


def postprocessor_runner(query: str, callback: Callable, logger) -> None:
    try:
        logger.debug('starting query %s', query)
        callback()
        logger.info('%s executed successfully', query)
    except Exception as e:
        logger.error('%s generated an exception: %s', query, str(e))


def init_logging(loglevel: str = 'INFO',
                 logger_type: str = 'local',
                 name: str = __name__) -> logging.Logger:
    if logger_type == 'rich':
        logging.basicConfig(
            format='%(message)s',
            level=loglevel,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                rich.logging.RichHandler(rich_tracebacks=True),
            ])
    else:
        logging.basicConfig(
            format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s',
            stream=sys.stdout,
            level=loglevel,
            datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger('google.ads.googleads.client').setLevel(logging.WARNING)
    logging.getLogger('smart_open.smart_open_lib').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    return logging.getLogger(name)
