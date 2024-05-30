from __future__ import annotations

import pytest

from gaarf.api_clients import GOOGLE_ADS_API_VERSION
from gaarf.query_editor import QuerySpecification
from gaarf.simulation import SimulatorSpecification, simulate_data


@pytest.fixture
def query():
  return """
        SELECT
            segments.date AS date,
            campaign.advertising_channel_sub_type AS campaign_type,
            campaign.id as campaign_id,
            campaign.name AS campaign_name,
            asset.youtube_video_asset.youtube_video_id AS video_id,
            asset.image_asset.full_size.url AS url,
            metrics.cost_micros AS cost,
            metrics.clicks AS clicks,
            metrics.conversions AS conversions
        FROM campaign
        """


class TestDefaultSimulation:
  @pytest.fixture
  def query_specification(self, query):
    return QuerySpecification(query).generate()

  @pytest.fixture
  def default_simulator_specification(self):
    return SimulatorSpecification(n_rows=3)

  @pytest.fixture
  def default_report(self, query, default_simulator_specification):
    return simulate_data(
      query_text=query,
      query_name='test',
      args=None,
      api_version=GOOGLE_ADS_API_VERSION,
      simulator_specification=default_simulator_specification,
    )

  def test_simulate_data_returns_same_n_rows_as_in_simulation_specification(
    self, default_report, default_simulator_specification
  ):
    assert len(default_report) == default_simulator_specification.n_rows

  def test_simulate_data_return_correct_number_of_columns(
    self, default_report, query_specification
  ):
    assert len(default_report.results[0]) == len(query_specification.fields)

  def test_simulate_data_return_correct_id_column(
    self, default_report, query_specification
  ):
    assert 1000000 <= default_report[0].campaign_id <= 1000010

  def test_simulate_data_return_correct_video_id_column(
    self, default_report, query_specification
  ):
    assert default_report[0].video_id == '4WXs3sKu41I'

  def test_simulate_data_return_correct_url_column(
    self, default_report, query_specification
  ):
    assert default_report[0].url == 'example.com'

  def test_simulate_data_return_correct_micros_column(
    self, default_report, query_specification
  ):
    assert 0 <= default_report[0].cost <= 1000 * 1e6

  def test_simulate_data_return_correct_int_column(
    self, default_report, query_specification
  ):
    assert 0 <= default_report[0].clicks <= 1000

  def test_simulate_data_return_correct_float_column(
    self, default_report, query_specification
  ):
    assert 0 <= default_report[0].conversions <= 1000


class TestCustomSimulation:
  @pytest.fixture
  def allowed_enums(self):
    return ['APP_CAMPAIGN', 'APP_CAMPAIGN_FOR_ENGAGEMENT']

  @pytest.fixture
  def replacements(self):
    return {'asset.youtube_video_asset.youtube_video_id': ['12345', '54321']}

  @pytest.fixture
  def custom_simulator_specification(self, allowed_enums, replacements):
    return SimulatorSpecification(
      allowed_enums={'campaign.advertising_channel_sub_type': allowed_enums},
      replacements=replacements,
    )

  @pytest.fixture
  def custom_report(self, query, custom_simulator_specification):
    return simulate_data(
      query_text=query,
      query_name='test',
      args=None,
      api_version=GOOGLE_ADS_API_VERSION,
      simulator_specification=custom_simulator_specification,
    )

  def test_simulate_data_returns_only_allowed_enums(
    self, custom_report, custom_simulator_specification, allowed_enums
  ):
    report_iterator = iter(custom_report)
    assert next(report_iterator).get('campaign_type') in allowed_enums
    assert next(report_iterator).get('campaign_type') in allowed_enums

  def test_simulate_data_return_correct_replacements(
    self, custom_report, custom_simulator_specification, replacements
  ):
    report_iterator = iter(custom_report)
    replacement = replacements.get('asset.youtube_video_asset.youtube_video_id')
    assert next(report_iterator).get('video_id') in replacement
    assert next(report_iterator).get('video_id') in replacement


class TestSimulationForBuiltInQuery:
  def test_simulate_data_returns_none_for_builtin_query(self):
    query = 'SELECT * FROM builtin.ocid_mapping'
    report = simulate_data(
      query_text=query,
      query_name='test',
      args=None,
      api_version=GOOGLE_ADS_API_VERSION,
      simulator_specification=SimulatorSpecification(),
    )
    assert report is None
