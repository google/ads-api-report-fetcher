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

from typing import Any, Callable, Dict, List, Optional, Sequence, Union
import dataclasses
import os
import datetime
from dateutil.relativedelta import relativedelta
import yaml
from google.ads.googleads.errors import GoogleAdsException  # type: ignore
import traceback

from gaarf.io.writer import ZeroRowException

@dataclasses.dataclass
class GaarfConfig:
    output: str
    api_version: str
    account: str
    params: Dict[str, Any]
    writer_params: Dict[str, Any]
    customer_ids_query: str
    customer_ids_query_file: str


@dataclasses.dataclass
class GaarfBqConfig:
    project: str
    params: Dict[str, Any]


class BaseConfigBuilder:

    def __init__(self, args):
        self.args = args
        if (gaarf_config_path := self.args[0].gaarf_config):
            self.type = "file"
            self.gaarf_config_path = gaarf_config_path
        else:
            self.type = "cli"
            self.gaarf_config_path = None

    def build(self) -> Union[GaarfConfig, GaarfBqConfig]:
        if self.type == "file":
            return self._load_gaarf_config()
        return self._build_gaarf_config()

    def _load_gaarf_config(self) -> Union[GaarfConfig, GaarfBqConfig]:
        raise NotImplementedError

    def _build_gaarf_config(self) -> Union[GaarfConfig, GaarfBqConfig]:
        raise NotImplementedError


class GaarfConfigBuilder(BaseConfigBuilder):

    def __init__(self, args):
        super().__init__(args)

    def _load_gaarf_config(self) -> GaarfConfig:
        with open(self.gaarf_config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        gaarf_section = config.get("gaarf")
        if not gaarf_section:
            raise ValueError("Invalid config, must have `gaarf` section!")
        if not (output := gaarf_section.get("output")):
            raise ValueError("Config does not contains `output` section!")
        return GaarfConfig(
            output=gaarf_section.get("output"),
            api_version=gaarf_section.get("api_version"),
            account=gaarf_section.get("account"),
            params=gaarf_section.get("params"),
            writer_params=gaarf_section.get(output),
            customer_ids_query=gaarf_section.get("customer_ids_query"),
            customer_ids_query_file=gaarf_section.get(
                "customer_ids_query_file"))

    def _build_gaarf_config(self) -> GaarfConfig:
        main_args, query_args = self.args[0], self.args[1]
        params = ParamsParser(["macro", main_args.save]).parse(query_args)
        return GaarfConfig(
            output=main_args.save,
            api_version=main_args.api_version,
            account=main_args.customer_id,
            params=params,
            writer_params=params.get(main_args.save),
            customer_ids_query=main_args.customer_ids_query,
            customer_ids_query_file=main_args.customer_ids_query_file)


class GaarfBqConfigBuilder(BaseConfigBuilder):

    def __init__(self, args):
        super().__init__(args)

    def _load_gaarf_config(self) -> GaarfBqConfig:
        with open(self.gaarf_config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        gaarf_section = config.get("gaarf-bq")
        if not gaarf_section:
            raise ValueError("Invalid config, must have `gaarf-bq` section!")
        params = gaarf_section.get("params")
        return GaarfBqConfig(project=gaarf_section.get("project"),
                             params=params)

    def _build_gaarf_config(self) -> GaarfBqConfig:
        main_args, query_args = self.args[0], self.args[1]
        params = ParamsParser(["macro", "sql", "template"]).parse(query_args)
        return GaarfBqConfig(project=main_args.project,
                             params=params)


class ParamsParser:

    common_params = {"date_iso": datetime.date.today().strftime("%Y%m%d")}

    def __init__(self, identifiers: Sequence[str]):
        self.identifiers = identifiers

    def parse(self, params: Sequence[Any]) -> Dict[str, Optional[Dict[str, Any]]]:
        return {
            identifier: self._parse_params(identifier, params)
            for identifier in self.identifiers
        }

    def _parse_params(self, identifier: str,
                      params: Sequence[Any]) -> Dict[Any, Any]:
        parsed_params = {}
        if params:
            raw_params = [param.split("=", maxsplit=1) for param in params]
            for param in raw_params:
                param_pair = self._identify_param_pair(identifier, param)
                if param_pair:
                    parsed_params.update(param_pair)
        return parsed_params

    def _identify_param_pair(self, identifier: str,
                             param: Sequence[str]) -> Optional[Dict[str, Any]]:
        key = param[0]
        if identifier not in key:
            return None
        key = key.replace(f"--{identifier}.", "")
        key = key.replace("-", "_")
        if len(param) == 2:
            return {key: param[1]}
        raise ValueError(f"{identifier} {key} is invalid,"
                         f"--{identifier}.key=value is the correct format")


def convert_date(date_string: str) -> str:
    """Converts specific dates parameters to actual dates."""

    if date_string.find(":YYYY") == -1:
        return date_string
    current_date = datetime.date.today()
    date_object = date_string.split("-")
    base_date = date_object[0]
    if len(date_object) == 2:
        try:
            days_ago = int(date_object[1])
        except ValueError as e:
            raise ValueError(
                "Must provide numeric value for a number lookback period, "
                "i.e. :YYYYMMDD-1") from e
    else:
        days_ago = 0
    if base_date == ":YYYY":
        new_date = datetime.datetime(current_date.year, 1, 1)
        delta = relativedelta(years=days_ago)
    elif base_date == ":YYYYMM":
        new_date = datetime.datetime(current_date.year, current_date.month, 1)
        delta = relativedelta(months=days_ago)
    elif base_date == ":YYYYMMDD":
        new_date = current_date
        delta = relativedelta(days=days_ago)
    return (new_date - delta).strftime("%Y-%m-%d")


class ConfigSaver:

    def __init__(self, path: str):
        self.path = path

    def save(self, gaarf_config: Union[GaarfConfig, GaarfBqConfig]):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        else:
            config = {}
        config = self.prepare_config(config, gaarf_config)
        with open(self.path, "w", encoding="utf-8") as f:
            yaml.dump(config,
                      f,
                      default_flow_style=False,
                      sort_keys=False,
                      encoding="utf-8")

    def prepare_config(self, config: Dict[Any, Any],
                       gaarf_config: Union[GaarfConfig, GaarfBqConfig]):
        gaarf = dataclasses.asdict(gaarf_config)
        if isinstance(gaarf_config, GaarfConfig):
            gaarf[gaarf_config.output] = gaarf_config.writer_params
            del gaarf["writer_params"]
            if gaarf_config.writer_params:
                del gaarf["params"][gaarf_config.output]
            gaarf = _remove_empty_values(gaarf)
            config.update({"gaarf": gaarf})
        if isinstance(gaarf_config, GaarfBqConfig):
            gaarf = _remove_empty_values(gaarf)
            config.update({"gaarf-bq": gaarf})
        return config

    @staticmethod
    def _handle_target_param(gaarf_config: GaarfBqConfig,
                             target: List[str]) -> List[str]:
        if isinstance(gaarf_config.target, str):
            target.append(gaarf_config.target)
        elif isinstance(gaarf_config.target, list):
            target.extend(gaarf_config.target)
        return target


def initialize_runtime_parameters(config: Union[GaarfConfig, GaarfBqConfig]):
    for key, param in config.params.items():
        for key_param, value_param in param.items():
            config.params[key][key_param] = convert_date(value_param)
        config.params[key].update(ParamsParser.common_params)
    return config


def _remove_empty_values(dict_object: Dict[str, Any]) -> Dict[str, Any]:
    """Remove all empty elements: strings, dictionaries from a dictionary."""
    if isinstance(dict_object, dict):
        return {
            key: value
            for key, value in ((key, _remove_empty_values(value))
                               for key, value in dict_object.items())
            if value
        }
    if isinstance(dict_object, (int, str)):
        return dict_object


def gaarf_runner(query: str, callback: Callable, logger) -> None:
    try:
        logger.debug("starting query %s", query)
        callback()
        logger.info("%s executed successfully", query)
    except ZeroRowException:
        logger.warning("%s returns 0 rows", query)
    except GoogleAdsException as ex:
        logger.error(
            "%s failed with status %s and includes the following errors:",
            query,
            ex.error.code().name)
        for error in ex.failure.errors:
            logger.error("\tError with message %s .",
                         error.message)
            if error.location:
                for field in error.location.field_path_elements:
                    logger.error("\t\tOn field %s",
                                 field.field_name)
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        logger.error("%s generated an exception: %s", query,
                     str(e))
