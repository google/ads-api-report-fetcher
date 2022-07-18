import dataclasses
import os
import datetime
from typing import Any, Dict, Optional, Sequence
import yaml


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
        key = key.replace("-","_")
        if len(param) == 2:
            return {key: param[1]}
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

    def save(self, args, params, key):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        else:
            config = {}
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
        with open(self.path, "w", encoding="utf-8") as f:
            yaml.dump(config,
                      f,
                      default_flow_style=False,
                      sort_keys=False,
                      encoding="utf-8")
