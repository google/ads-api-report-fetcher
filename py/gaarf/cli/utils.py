import dataclasses
import os
import datetime
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, Optional, Sequence
import yaml


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


class ParamsParser:

    common_params = {"date_iso": datetime.date.today().strftime("%Y%m%d")}

    def __init__(self, identifiers: Sequence[str]):
        self.identifiers = identifiers

    def parse(self,
              params: Sequence[Any]) -> Dict[str, Optional[Dict[str, Any]]]:
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
            parsed_params.update(self.common_params)
        return parsed_params

    def _identify_param_pair(self, identifier: str,
                             param: Sequence[str]) -> Optional[Dict[str, Any]]:
        key = param[0]
        if identifier not in key:
            return None
        key = key.replace(f"--{identifier}.", "")
        key = key.replace("-", "_")
        if len(param) == 2:
            return {key: convert_date(param[1])}
        raise ValueError(f"{identifier} {key} is invalid,"
                         f"--{identifier}.key=value is the correct format")


@dataclasses.dataclass
class ExecutorParams:
    sql_params: Dict[str, Any]
    macro_params: Dict[str, Any]
    template_params: Dict[str, Any]


class ExecutorParamsParser:

    def __init__(self, params: Dict[str, Any]):
        self.params = params

    def parse(self) -> ExecutorParams:
        return ExecutorParams(sql_params=self.params.get("sql"),
                              macro_params=self.params.get("macro"),
                              template_params=self.params.get("template"))


class WriterParamsParser:

    def __init__(self, params: Dict[str, Any]):
        self.params = params

    def parse(self, key: str) -> Optional[Dict[str, Any]]:
        return self.params.get(key)


class ConfigSaver:

    def __init__(self, path: str):
        self.path = path

    #TODO: Add support for AbsReader
    def save(self, args, params: Dict[str, Any], key: str):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        else:
            config = {}
        config = self.prepare_config(config, args, params, key)
        with open(self.path, "w", encoding="utf-8") as f:
            yaml.dump(config,
                      f,
                      default_flow_style=False,
                      sort_keys=False,
                      encoding="utf-8")


    def prepare_config(self, config: Dict[str, Any], args, params: Dict[str, Any],
                key: str):
        if key == "gaarf":
            gaarf = {}
            gaarf["account"] = args.customer_id
            gaarf["output"] = args.save
            gaarf[args.save] = params.get(args.save)
            gaarf["params"] = params
            gaarf["api-version"] = args.api_version
            del gaarf["params"][args.save]
            config.update({key: gaarf})
        if key == "gaarf-bq":
            bq = {}
            bq["project"] = args.project
            bq["params"] = params
            config.update({key: bq})
        return config
