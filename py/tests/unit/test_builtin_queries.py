from __future__ import annotations

import pytest

from gaarf import api_clients, builtin_queries, report, report_fetcher


class TestBuiltInQueries:
  @pytest.fixture
  def test_client(self, mocker, config_path):
    mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
    return api_clients.GoogleAdsApiClient(path_to_config=config_path)

  @pytest.fixture
  def fake_report_fetcher(self, mocker, test_client):
    data = [
      [
        1,
        'https://ads.google.com/aw/accounts?__u=1&&ocid=12345',
      ],
    ]
    column_names = [
      'account_id',
      'url',
    ]
    mocker.patch(
      'gaarf.report_fetcher.AdsReportFetcher.fetch',
      return_value=report.GaarfReport(data, column_names),
    )
    return report_fetcher.AdsReportFetcher(test_client)

  @pytest.fixture
  def missing_ocid_fake_report_fetcher(self, mocker, test_client):
    data = [
      [
        1,
        '',
      ],
    ]
    column_names = [
      'account_id',
      'url',
    ]
    mocker.patch(
      'gaarf.report_fetcher.AdsReportFetcher.fetch',
      return_value=report.GaarfReport(data, column_names),
    )
    return report_fetcher.AdsReportFetcher(test_client)

  @pytest.fixture
  def empty_fake_report_fetcher(self, mocker, test_client):
    column_names = [
      'account_id',
      'url',
    ]
    mocker.patch(
      'gaarf.report_fetcher.AdsReportFetcher.fetch',
      return_value=report.GaarfReport([], column_names),
    )
    return report_fetcher.AdsReportFetcher(test_client)

  def test_get_ocid_mapping_returns_correct_result(self, fake_report_fetcher):
    account_id = 1
    expected_report = report.GaarfReport(
      results=[
        [
          account_id,
          '12345',
        ],
      ],
      column_names=[
        'account_id',
        'ocid',
      ],
    )
    ocid_mapping = builtin_queries.get_ocid_mapping(
      fake_report_fetcher,
      accounts=[
        account_id,
      ],
    )
    assert ocid_mapping == expected_report

  def test_get_ocid_mapping_returns_placeholder_for_missing_ocid_report(
    self, missing_ocid_fake_report_fetcher
  ):
    account_id = 1
    expected_report = report.GaarfReport(
      results=[
        [
          account_id,
          '0',
        ],
      ],
      column_names=[
        'account_id',
        'ocid',
      ],
    )
    ocid_mapping = builtin_queries.get_ocid_mapping(
      missing_ocid_fake_report_fetcher,
      accounts=[
        account_id,
      ],
    )
    assert ocid_mapping == expected_report

  def test_get_ocid_mapping_returns_placeholder_for_empty_report(
    self, empty_fake_report_fetcher
  ):
    account_id = 1
    expected_report = report.GaarfReport(
      results=[
        [
          account_id,
          '0',
        ],
      ],
      column_names=[
        'account_id',
        'ocid',
      ],
    )
    ocid_mapping = builtin_queries.get_ocid_mapping(
      empty_fake_report_fetcher,
      accounts=[
        account_id,
      ],
    )
    assert ocid_mapping == expected_report
