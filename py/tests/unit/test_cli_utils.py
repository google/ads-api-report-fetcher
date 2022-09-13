import pytest

from datetime import datetime
from dateutil.relativedelta import relativedelta
import gaarf.cli.utils as utils


def test_convert_date():
    current_date = datetime.today()
    current_year = datetime(current_date.year, 1, 1)
    current_month = datetime(current_date.year, current_date.month, 1)
    last_year = current_year - relativedelta(years=1)
    last_month = current_month - relativedelta(months=1)
    yesterday = current_date - relativedelta(days=1)

    non_macro_date = "2022-01-01"
    date_year = ":YYYY"
    date_month = ":YYYYMM"
    date_day = ":YYYYMMDD"
    date_year_minus_one = ":YYYY-1"
    date_month_minus_one = ":YYYYMM-1"
    date_day_minus_one = ":YYYYMMDD-1"

    non_macro_date_converted = utils.convert_date(non_macro_date)
    new_date_year = utils.convert_date(date_year)
    new_date_month = utils.convert_date(date_month)
    new_date_day = utils.convert_date(date_day)
    new_date_year_minus_one = utils.convert_date(date_year_minus_one)
    new_date_month_minus_one = utils.convert_date(date_month_minus_one)
    new_date_day_minus_one = utils.convert_date(date_day_minus_one)

    assert non_macro_date_converted == non_macro_date
    assert new_date_year == current_year.strftime("%Y-%m-%d")
    assert new_date_month == current_month.strftime("%Y-%m-%d")
    assert new_date_day == current_date.strftime("%Y-%m-%d")
    assert new_date_year_minus_one == last_year.strftime("%Y-%m-%d")
    assert new_date_month_minus_one == last_month.strftime("%Y-%m-%d")
    assert new_date_day_minus_one == yesterday.strftime("%Y-%m-%d")


def test_wrong_convert_date():
    date_day = ":YYYYMMDD-N"
    with pytest.raises(ValueError):
        utils.convert_date(date_day)


@pytest.fixture
def param_parser():
    return utils.ParamsParser(["macro", "sql", "template"])


def test_identify_param_pair_existing(param_parser):
    param_pair = param_parser._identify_param_pair(
        "macro", ["--macro.start_date", "2022-01-01"])
    assert param_pair == {"start_date": "2022-01-01"}


def test_identify_param_pair_empty(param_parser):
    param_pair = param_parser._identify_param_pair(
        "macro", ["--missing_param.start_date", "2022-01-01"])
    assert param_pair is None


def test_identify_param_pair_raises_error(param_parser):
    with pytest.raises(ValueError):
        param_parser._identify_param_pair(
            "macro", ["--macro.start_date", ":YYYYMMDD", "extra_element"])


@pytest.fixture
def current_date_iso():
    return datetime.today().strftime("%Y%m%d")


def test_parse_params(param_parser, current_date_iso):
    parsed_params = param_parser._parse_params(
        "macro",
        ["--macro.start_date=2022-01-01", "--macro.end_date=2022-12-31"])
    assert parsed_params == {
        "start_date": "2022-01-01",
        "end_date": "2022-12-31"
    }


def test_parse(param_parser, current_date_iso):
    parsed_params = param_parser.parse(
        ["--macro.start_date=2022-01-01", "--macro.end_date=2022-12-31"])
    assert parsed_params == {
        "macro": {
            "start_date": "2022-01-01",
            "end_date": "2022-12-31"
        },
        "sql": {},
        "template": {},
    }


@pytest.fixture
def config_args():
    from dataclasses import dataclass

    @dataclass
    class FakeConfig:
        customer_id: str
        save: str
        api_version: str
        project: str

    return FakeConfig("1", "console", "10", "fake-project")


@pytest.fixture
def config_saver(config_args):
    return utils.ConfigSaver("/tmp/config.yaml")


def test_gaarf_config_saver_gaarf_dont_save_empty_values(config_saver):
    gaarf_config = utils.GaarfConfig(output="console",
                                     api_version="10",
                                     account="1",
                                     params={},
                                     writer_params={},
                                     customer_ids_query="",
                                     customer_ids_query_file="")

    config = config_saver.prepare_config({}, gaarf_config)
    assert config == {
        "gaarf": {
            "account": "1",
            "output": "console",
            "api_version": "10",
        }
    }


def test_gaarf_config_saver_gaarf_dont_save_inner_empty_values(config_saver):
    gaarf_config = utils.GaarfConfig(output="console",
                                     api_version="10",
                                     account="1",
                                     params={"macro": {
                                         "start_date": ":YYYYMMDD"
                                     }},
                                     writer_params={},
                                     customer_ids_query="",
                                     customer_ids_query_file="")

    config = config_saver.prepare_config({}, gaarf_config)
    assert config == {
        "gaarf": {
            "account": "1",
            "output": "console",
            "api_version": "10",
            "params": {
                "macro": {
                    "start_date": ":YYYYMMDD"
                }
            }
        }
    }


def test_config_saver_gaarf_save_customer_ids_query_values(config_saver):
    gaarf_config = utils.GaarfConfig(output="console",
                                     api_version="10",
                                     account="1",
                                     params={},
                                     writer_params={},
                                     customer_ids_query="SELECT",
                                     customer_ids_query_file="path/to/file.sql")

    config = config_saver.prepare_config({}, gaarf_config)
    assert config == {
        "gaarf": {
            "account": "1",
            "output": "console",
            "api_version": "10",
            "customer_ids_query": "SELECT",
            "customer_ids_query_file": "path/to/file.sql"
        }
    }


def test_config_saver_gaarf_bq(config_saver):
    gaarf_bq_config = utils.GaarfBqConfig(
        project="fake-project",
        params={"macro": {"bq_project": "another-fake-project"}})
    config = config_saver.prepare_config({}, gaarf_bq_config)
    assert config == {
        "gaarf-bq": {
            "project": "fake-project",
            "params": {
                "macro": {
                    "bq_project": "another-fake-project"
                }
            }
        }
    }


def test_config_saver_does_not_save_empty_params(config_saver):
    gaarf_bq_config = utils.GaarfBqConfig(
        project="fake-project",
        params={})
    config = config_saver.prepare_config({}, gaarf_bq_config)
    assert config == {
        "gaarf-bq": {
            "project": "fake-project",
        }
    }


def test_config_saver_does_not_save_empty_nested_params(config_saver):
    gaarf_bq_config = utils.GaarfBqConfig(
        project="fake-project",
        params={"macro": {"bq_project": "another-fake-project"}, "sql": {}})
    config = config_saver.prepare_config({}, gaarf_bq_config)
    assert config == {
        "gaarf-bq": {
            "project": "fake-project",
            "params": {
                "macro": {
                    "bq_project": "another-fake-project"
                }
            }
        }
    }


@pytest.fixture
def config_with_runtime_params():
    return utils.GaarfConfig(output="console",
                             api_version="10",
                             account="1",
                             params={"macro": {
                                 "start_date": ":YYYYMMDD"
                             }},
                             writer_params={},
                             customer_ids_query=None,
                             customer_ids_query_file=None)


@pytest.fixture
def config_without_runtime_params():
    return utils.GaarfConfig(output="console",
                             api_version="10",
                             account="1",
                             params={"macro": {
                                 "start_date": "2022-01-01"
                             }},
                             writer_params={},
                             customer_ids_query=None,
                             customer_ids_query_file=None)


def test_initialize_config_with_runtime_parameters(config_with_runtime_params,
                                                   current_date_iso):
    initialized_config = utils.initialize_runtime_parameters(
        config_with_runtime_params)
    expected_config = utils.GaarfConfig(
        output="console",
        api_version="10",
        account="1",
        params={
            "macro": {
                "start_date": datetime.today().strftime("%Y-%m-%d"),
                "date_iso": current_date_iso
            }
        },
        writer_params={},
        customer_ids_query=None,
        customer_ids_query_file=None)
    assert initialized_config == expected_config


def test_initialize_config_without_runtime_parameters(
        config_without_runtime_params, current_date_iso):
    initialized_config = utils.initialize_runtime_parameters(
        config_without_runtime_params)
    expected_config = utils.GaarfConfig(output="console",
                                        api_version="10",
                                        account="1",
                                        params={
                                            "macro": {
                                                "start_date": "2022-01-01",
                                                "date_iso": current_date_iso
                                            }
                                        },
                                        writer_params={},
                                        customer_ids_query=None,
                                        customer_ids_query_file=None)
    assert initialized_config == expected_config
