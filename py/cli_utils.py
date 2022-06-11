import datetime
from typing import Any, Dict, Optional, Sequence


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
            parsed_params.update(self.common_params)
        return parsed_params

    def _identify_param_pair(self, identifier: str,
                             param: Sequence[str]) -> Optional[Dict[str, Any]]:
        key = param[0]
        if identifier not in key:
            return None
        key = key.replace(f"--{identifier}.", "")
        if len(param) == 2:
            return {key: param[1]}
        raise ValueError(f"{identifier} {key} is invalid,"
                         f"--{identifier}.key=value is the correct format")
