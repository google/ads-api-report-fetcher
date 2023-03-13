from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Sequence, Tuple, Union
import random
import string
from faker import Faker

from gaarf.report import GaarfReport
from gaarf.query_executor import AdsReportFetcher
from gaarf.api_clients import BaseClient
from gaarf.query_editor import QuerySpecification


@dataclass(frozen=True)
class FakeField:
    type: Any
    name: str


@dataclass
class SimulatorSpecification:
    api_version: str = "v12"
    n_rows: int = 1000
    days_ago: str = "-7d"
    string_length: int = 3
    ignored_enums: Tuple[str, ...] = ("REMOVED", "UNKNOWN", "UNSPECIFIED")
    allowed_enums: Optional[Dict[str, Sequence[str]]] = None
    replacements: Optional[Dict[str, Any]] = None


def simulate_data(
    query: str,
    simulator_specification: SimulatorSpecification,
    simulation_helper: Callable = Faker()
) -> GaarfReport:
    query_specification = QuerySpecification(query).generate()
    client = BaseClient(simulator_specification.api_version)
    report_fetcher = AdsReportFetcher(client, [])
    other_types = client.infer_types(query_specification.fields)
    v = report_fetcher.fetch(query_specification)
    try:
        iter(v.results[0])
        results = v.results[0]
    except TypeError:
        results = [v.results[0]]
    if not results:
        results = [results]
    types = [
        FakeField(type(field_value), field_name)
        for field_value, field_name in zip(results, query_specification.fields)
    ]
    values = []
    for _ in range(simulator_specification.n_rows):
        row = []
        for i, field in enumerate(types):
            if len(enums := other_types[i].values) > 1:
                if allowed_enums := simulator_specification.allowed_enums:
                    if selected_enums := allowed_enums.get(field.name):
                        value = random.choice(selected_enums)
                    else:
                        value = random.choice(list(enums))
                else:
                    value = random.choice([
                        value for value in other_types[i].values
                        if value not in simulator_specification.ignored_enums
                    ])
            elif (simulator_specification.replacements
                  and field.name in simulator_specification.replacements):
                value = random.choice(
                    simulator_specification.replacements[field.name])
            else:
                value = generate_random_value(
                    field, simulator_specification, simulation_helper,
                    query_specification.column_names[i])
            row.append(value)
        values.append(row)
    return GaarfReport(values, query_specification.column_names)


def generate_random_value(field: FakeField,
                          simulator_specification: SimulatorSpecification,
                          simulation_helper: Callable,
                          column_name: str) -> Union[bool, str, float]:
    if field.type == str:
        if "date" in field.name:
            return simulation_helper.date_between(
                simulator_specification.days_ago).strftime("%Y-%m-%d")
        if "url" in field.name:
            return "example.com"
        if "video.id" in field.name or "video_id" in field.name:
            return "4WXs3sKu41I"
        if "id" in column_name:
            return str(random.randint(1000000, 1000010))
        return generate_random_string(simulator_specification.string_length)
    if field.type == int:
        if ".id" in field.name:
            return random.randint(1000000, 1000010)
        if "micros" in field.name:
            return int(random.randint(0, 1000) * 1e6)
        return random.randint(0, 1000)
    if field.type == float:
        return float(random.randint(0, 1000))
    if field.type == bool:
        return bool(random.getrandbits(1))
    return ""


def generate_random_string(length: int) -> str:
    characters = string.ascii_lowercase
    return ''.join(random.choice(characters) for _ in range(length))
