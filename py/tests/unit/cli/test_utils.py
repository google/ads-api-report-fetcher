# Copyright 2024 Google LLC
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

from datetime import datetime
from unittest import mock

import pytest
import yaml
from dateutil.relativedelta import relativedelta

from gaarf import api_clients
from gaarf.cli import utils
from gaarf.query_editor import CommonParametersMixin


class TestGaarfConfig:
  @pytest.mark.parametrize(
    'input_account,output_account',
    [
      (
        None,
        None,
      ),
      (
        '123-456-7890',
        '1234567890',
      ),
      (
        [
          '123-456-7890',
        ],
        [
          '1234567890',
        ],
      ),
    ],
  )
  def test_post_init_returns_correctly_formatted_account(
    self, input_account, output_account
  ):
    config = utils.GaarfConfig(
      output='console',
      api_version=api_clients.GOOGLE_ADS_API_VERSION,
      account=input_account,
    )
    assert config.account == output_account

  def test_post_init_returns_correctly_formatted_writer_params(self):
    config = utils.GaarfConfig(
      output='console',
      api_version=api_clients.GOOGLE_ADS_API_VERSION,
      account=None,
      writer_params={
        'page-size': 10,
      },
    )
    assert 'page_size' in config.writer_params


def test_convert_date():
  current_date = datetime.today()
  current_year = datetime(current_date.year, 1, 1)
  current_month = datetime(current_date.year, current_date.month, 1)
  last_year = current_year - relativedelta(years=1)
  last_month = current_month - relativedelta(months=1)
  yesterday = current_date - relativedelta(days=1)

  non_macro_date = '2022-01-01'
  date_year = ':YYYY'
  date_month = ':YYYYMM'
  date_day = ':YYYYMMDD'
  date_year_minus_one = ':YYYY-1'
  date_month_minus_one = ':YYYYMM-1'
  date_day_minus_one = ':YYYYMMDD-1'

  non_macro_date_converted = utils.convert_date(non_macro_date)
  new_date_year = utils.convert_date(date_year)
  new_date_month = utils.convert_date(date_month)
  new_date_day = utils.convert_date(date_day)
  new_date_year_minus_one = utils.convert_date(date_year_minus_one)
  new_date_month_minus_one = utils.convert_date(date_month_minus_one)
  new_date_day_minus_one = utils.convert_date(date_day_minus_one)

  assert non_macro_date_converted == non_macro_date
  assert new_date_year == current_year.strftime('%Y-%m-%d')
  assert new_date_month == current_month.strftime('%Y-%m-%d')
  assert new_date_day == current_date.strftime('%Y-%m-%d')
  assert new_date_year_minus_one == last_year.strftime('%Y-%m-%d')
  assert new_date_month_minus_one == last_month.strftime('%Y-%m-%d')
  assert new_date_day_minus_one == yesterday.strftime('%Y-%m-%d')


def test_wrong_convert_date():
  date_day = ':YYYYMMDD-N'
  with pytest.raises(ValueError):
    utils.convert_date(date_day)


@pytest.fixture
def param_parser():
  return utils.ParamsParser(['macro', 'template'])


def test_if_incorrect_param_is_provided(param_parser):
  with pytest.raises(utils.GaarfParamsException):
    parsed_params = param_parser.parse(
      ['--macros.start_date=2022-01-01', '--fake_param.end_date=2022-12-31']
    )


def test_identify_param_pair_existing(param_parser):
  param_pair = param_parser._identify_param_pair(
    'macro', ['--macro.start_date', '2022-01-01']
  )
  assert param_pair == {'start_date': '2022-01-01'}


def test_identify_param_pair_empty(param_parser):
  param_pair = param_parser._identify_param_pair(
    'macro', ['--missing_param.start_date', '2022-01-01']
  )
  assert param_pair is None


def test_identify_param_pair_raises_error(param_parser):
  with pytest.raises(utils.GaarfParamsException):
    param_parser._identify_param_pair(
      'macro', ['--macro.start_date', ':YYYYMMDD', 'extra_element']
    )


def test_parse_params(param_parser):
  parsed_params = param_parser._parse_params(
    'macro', ['--macro.start_date=2022-01-01', '--macro.end_date=2022-12-31']
  )
  assert parsed_params == {'start_date': '2022-01-01', 'end_date': '2022-12-31'}


def test_parse(param_parser):
  parsed_params = param_parser.parse(
    ['--macro.start_date=2022-01-01', '--macro.end_date=2022-12-31']
  )
  assert parsed_params == {
    'macro': {'start_date': '2022-01-01', 'end_date': '2022-12-31'},
    'template': {},
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

  return FakeConfig('1', 'console', '10', 'fake-project')


@pytest.fixture
def config_saver(config_args):
  return utils.ConfigSaver('/tmp/config.yaml')


def test_gaarf_config_saver_gaarf_transforms_comma_separated_account_into_list(
  config_saver,
):
  gaarf_config = utils.GaarfConfig(
    output='console', api_version='10', account='1-,2,3'
  )

  config = config_saver.prepare_config({}, gaarf_config)
  assert config == {
    'gaarf': {
      'account': ['1', '2', '3'],
      'output': 'console',
      'api_version': '10',
    }
  }


def test_gaarf_config_saver_gaarf_dont_save_empty_values(config_saver):
  gaarf_config = utils.GaarfConfig(
    output='console',
    api_version='10',
    account='1',
    params={},
    writer_params={},
    customer_ids_query='',
    customer_ids_query_file='',
  )

  config = config_saver.prepare_config({}, gaarf_config)
  assert config == {
    'gaarf': {
      'account': ['1'],
      'output': 'console',
      'api_version': '10',
    }
  }


def test_gaarf_config_saver_gaarf_dont_save_inner_empty_values(config_saver):
  gaarf_config = utils.GaarfConfig(
    output='console',
    api_version='10',
    account='1',
    params={'macro': {'start_date': ':YYYYMMDD'}},
    writer_params={},
    customer_ids_query='',
    customer_ids_query_file='',
  )

  config = config_saver.prepare_config({}, gaarf_config)
  assert config == {
    'gaarf': {
      'account': ['1'],
      'output': 'console',
      'api_version': '10',
      'params': {'macro': {'start_date': ':YYYYMMDD'}},
    }
  }


def test_config_saver_gaarf_save_customer_ids_query_values(config_saver):
  gaarf_config = utils.GaarfConfig(
    output='console',
    api_version='10',
    account='1',
    params={},
    writer_params={},
    customer_ids_query='SELECT',
    customer_ids_query_file='path/to/file.sql',
  )

  config = config_saver.prepare_config({}, gaarf_config)
  assert config == {
    'gaarf': {
      'account': ['1'],
      'output': 'console',
      'api_version': '10',
      'customer_ids_query': 'SELECT',
      'customer_ids_query_file': 'path/to/file.sql',
    }
  }


def test_config_saver_gaarf_bq(config_saver):
  gaarf_bq_config = utils.GaarfBqConfig(
    project='fake-project',
    dataset_location='fake-location',
    params={'macro': {'bq_project': 'another-fake-project'}},
  )
  config = config_saver.prepare_config({}, gaarf_bq_config)
  assert config == {
    'gaarf-bq': {
      'project': 'fake-project',
      'dataset_location': 'fake-location',
      'params': {'macro': {'bq_project': 'another-fake-project'}},
    }
  }


def test_config_saver_does_not_save_empty_params(config_saver):
  gaarf_bq_config = utils.GaarfBqConfig(
    project='fake-project', dataset_location='fake-location', params={}
  )
  config = config_saver.prepare_config({}, gaarf_bq_config)
  assert config == {
    'gaarf-bq': {'project': 'fake-project', 'dataset_location': 'fake-location'}
  }


def test_config_saver_does_not_save_empty_nested_params(config_saver):
  gaarf_bq_config = utils.GaarfBqConfig(
    project='fake-project',
    dataset_location='fake-location',
    params={
      'macro': {'bq_project': 'another-fake-project'},
    },
  )
  config = config_saver.prepare_config({}, gaarf_bq_config)
  assert config == {
    'gaarf-bq': {
      'project': 'fake-project',
      'dataset_location': 'fake-location',
      'params': {'macro': {'bq_project': 'another-fake-project'}},
    }
  }


def test_initialize_runtime_paramers_injects_common_parameters_to_present_macros(  # pylint: disable=line-to-long
):
  config = utils.GaarfConfig(
    output='console',
    api_version='10',
    account='1',
    params={'macro': {'start_date': ':YYYYMMDD'}},
    writer_params={},
    customer_ids_query=None,
    customer_ids_query_file=None,
  )
  with mock.patch.object(
    CommonParametersMixin,
    'common_params',
    {
      'date_iso': '19700101',
      'current_date': '1970-01-01',
      'current_datetime': '1970-01-01 00-00-00',
    },
  ):
    initialized_config = utils.initialize_runtime_parameters(config)
    expected_config = utils.GaarfConfig(
      output='console',
      api_version='10',
      account='1',
      params={
        'macro': {
          'start_date': datetime.today().strftime('%Y-%m-%d'),
          'date_iso': '19700101',
          'current_date': '1970-01-01',
          'current_datetime': '1970-01-01 00-00-00',
        }
      },
      writer_params={},
      customer_ids_query=None,
      customer_ids_query_file=None,
    )
    assert initialized_config == expected_config


def test_initialize_runtime_paramers_injects_common_parameters_to_empty_macros():  # pylint: disable=line-too-long
  config = utils.GaarfConfig(
    output='console',
    api_version='10',
    account='1',
    params={'macro': {'start_date': '2022-01-01'}},
    writer_params={},
    customer_ids_query=None,
    customer_ids_query_file=None,
  )
  with mock.patch.object(
    CommonParametersMixin,
    'common_params',
    {
      'date_iso': '19700101',
      'current_date': '1970-01-01',
      'current_datetime': '1970-01-01 00-00-00',
    },
  ):
    initialized_config = utils.initialize_runtime_parameters(config)
    expected_config = utils.GaarfConfig(
      output='console',
      api_version='10',
      account='1',
      params={
        'macro': {
          'start_date': '2022-01-01',
          'date_iso': '19700101',
          'current_date': '1970-01-01',
          'current_datetime': '1970-01-01 00-00-00',
        }
      },
      writer_params={},
      customer_ids_query=None,
      customer_ids_query_file=None,
    )
    assert initialized_config == expected_config


def test_initialize_runtime_parameters_overwrites_common_params():
  config = utils.GaarfConfig(
    output='console',
    api_version='10',
    account='1',
    params={
      'macro': {
        'start_date': '2022-01-01',
        'date_iso': '20220101',
      }
    },
    writer_params={},
    customer_ids_query=None,
    customer_ids_query_file=None,
  )
  with mock.patch.object(
    CommonParametersMixin,
    'common_params',
    {
      'date_iso': '19700101',
      'current_date': '1970-01-01',
      'current_datetime': '1970-01-01 00-00-00',
    },
  ):
    initialized_config = utils.initialize_runtime_parameters(config)
    expected_config = utils.GaarfConfig(
      output='console',
      api_version='10',
      account='1',
      params={
        'macro': {
          'start_date': '2022-01-01',
          'date_iso': '20220101',
          'current_date': '1970-01-01',
          'current_datetime': '1970-01-01 00-00-00',
        }
      },
      writer_params={},
      customer_ids_query=None,
      customer_ids_query_file=None,
    )
    assert initialized_config == expected_config


class TestConfigBuilder:
  @pytest.fixture
  def fake_config_path(self, tmp_path):
    config = {
      'gaarf': {
        'output': 'csv',
        'account': '123456789',
        'api_version': 'v16',
        'csv': {
          'destination_folder': '/home',
        },
      },
      'gaarf-bq': {
        'project': 'fake-project',
      },
      'gaarf-sql': {
        'connection_string': 'fake-connection-string',
      },
    }
    file = tmp_path / 'config.yaml'
    with open(file, mode='w', encoding='utf-8') as f:
      yaml.dump(
        config, f, default_flow_style=False, sort_keys=False, encoding='utf-8'
      )
    return file

  def test_build_raises_gaarf_config_exception_when_gaarf_section_is_missing(
    self,
  ):
    with pytest.raises(utils.GaarfConfigException):
      utils.ConfigBuilder(config_type='unknown-type')

  class TestGaarfConfig:
    def test_build_create_valid_config_from_config_file(self, fake_config_path):
      expected_config = utils.GaarfConfig(
        output='csv',
        api_version='v16',
        account='123456789',
        writer_params={'destination_folder': '/home'},
      )

      builder = utils.ConfigBuilder(config_type='gaarf')
      args = {'gaarf_config': fake_config_path}
      kwargs = []
      config = builder.build(args, kwargs)
      assert config == expected_config

    def test_build_create_valid_config_from_cli(self):
      expected_config = utils.GaarfConfig(
        output='csv',
        api_version=api_clients.GOOGLE_ADS_API_VERSION,
        account='123456789',
        params={
          'macro': {
            'start_date': '1970-01-01',
          },
          'template': {
            'skan4': 'true',
          },
        },
        writer_params={'destination_folder': '/srv'},
      )

      builder = utils.ConfigBuilder(config_type='gaarf')
      args = {'output': 'csv', 'account': 123456789}
      kwargs = [
        '--csv.destination-folder=/srv',
        '--macro.start_date=1970-01-01',
        '--template.skan4=true',
      ]
      config = builder.build(args, kwargs)
      assert config == expected_config

    def test_build_create_valid_config_from_config_file_and_cli(
      self, fake_config_path
    ):
      expected_config = utils.GaarfConfig(
        output='csv',
        api_version=api_clients.GOOGLE_ADS_API_VERSION,
        account='123456789',
        params={
          'macro': {
            'start_date': '1970-01-01',
          },
          'template': {
            'skan4': 'true',
          },
        },
        writer_params={'destination_folder': '/srv'},
      )

      builder = utils.ConfigBuilder(config_type='gaarf')
      args = {'gaarf_config': fake_config_path, 'output': 'csv'}
      kwargs = [
        '--csv.destination-folder=/srv',
        '--macro.start_date=1970-01-01',
        '--template.skan4=true',
      ]
      config = builder.build(args, kwargs)
      assert config == expected_config

  class TestGaarfBqConfig:
    def test_build_create_valid_config_file(self, fake_config_path):
      expected_config = utils.GaarfBqConfig(project='fake-project')
      builder = utils.ConfigBuilder(config_type='gaarf-bq')
      args = {'gaarf_config': fake_config_path}
      kwargs = []
      config = builder.build(args, kwargs)
      assert config == expected_config

    def test_build_create_valid_config_from_cli(self):
      expected_config = utils.GaarfBqConfig(
        project='fake-project',
        params={
          'macro': {
            'start_date': '1970-01-01',
          },
          'template': {
            'skan4': 'true',
          },
        },
      )

      builder = utils.ConfigBuilder(config_type='gaarf-bq')
      args = {'project': 'fake-project'}
      kwargs = [
        '--macro.start_date=1970-01-01',
        '--template.skan4=true',
      ]
      config = builder.build(args, kwargs)
      assert config == expected_config

    def test_build_create_valid_config_from_config_file_and_cli(
      self, fake_config_path
    ):
      expected_config = utils.GaarfBqConfig(
        project='new-fake-project',
        params={
          'macro': {
            'start_date': '1970-01-01',
          },
          'template': {
            'skan4': 'true',
          },
        },
      )

      builder = utils.ConfigBuilder(config_type='gaarf-bq')
      args = {'gaarf_config': fake_config_path, 'project': 'new-fake-project'}
      kwargs = [
        '--macro.start_date=1970-01-01',
        '--template.skan4=true',
      ]
      config = builder.build(args, kwargs)
      assert config == expected_config

  class TestGaarfSqlConfig:
    def test_build_create_valid_config_from_config_file(self, fake_config_path):
      expected_config = utils.GaarfSqlConfig(
        connection_string='fake-connection-string'
      )
      builder = utils.ConfigBuilder(config_type='gaarf-sql')
      args = {'gaarf_config': fake_config_path}
      kwargs = []
      config = builder.build(args, kwargs)
      assert config == expected_config

    def test_build_create_valid_config_from_cli(self):
      expected_config = utils.GaarfSqlConfig(
        connection_string='fake-connection-string',
        params={
          'macro': {
            'start_date': '1970-01-01',
          },
          'template': {
            'skan4': 'true',
          },
        },
      )
      builder = utils.ConfigBuilder(config_type='gaarf-sql')
      args = {'connection_string': 'fake-connection-string'}
      kwargs = [
        '--macro.start_date=1970-01-01',
        '--template.skan4=true',
      ]
      config = builder.build(args, kwargs)
      assert config == expected_config

    def test_build_create_valid_config_from_config_file_and_cli(
      self, fake_config_path
    ):
      expected_config = utils.GaarfSqlConfig(
        connection_string='new-fake-connection-string',
        params={
          'macro': {
            'start_date': '1970-01-01',
          },
          'template': {
            'skan4': 'true',
          },
        },
      )
      builder = utils.ConfigBuilder(config_type='gaarf-sql')
      args = {
        'gaarf_config': fake_config_path,
        'connection_string': 'new-fake-connection-string',
      }
      kwargs = [
        '--macro.start_date=1970-01-01',
        '--template.skan4=true',
      ]
      config = builder.build(args, kwargs)
      assert config == expected_config
