# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

from __future__ import annotations

import dataclasses
import itertools
import logging

import pytest
import tenacity
from google.ads.googleads import errors as googleads_exceptions
from google.api_core import exceptions as google_exceptions

from gaarf import (
  api_clients,
  parsers,
  query_editor,
  report,
  report_fetcher,
)
from tests.unit import helpers

_QUERY = 'SELECT customer.id AS customer_id FROM customer'
_EXPECTED_RESULTS = [
  [1],
  [2],
  [3],
]


@dataclasses.dataclass
class FakeGoogleAdsFailureMessage:
  message: str


@dataclasses.dataclass
class FakeGoogleAdsFailure:
  errors: list[FakeGoogleAdsFailureMessage]


class TestAdsReportFetcher:
  @pytest.fixture
  def fake_response(self):
    fake_results = [
      [
        helpers.FakeGoogleAdsRowElement(helpers.Customer(1)),
      ],
      [
        helpers.FakeGoogleAdsRowElement(helpers.Customer(2)),
      ],
      [
        helpers.FakeGoogleAdsRowElement(helpers.Customer(3)),
      ],
    ]
    return helpers.FakeResponse(data=fake_results)

  @pytest.fixture
  def test_client(self, mocker, config_path):
    mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
    return api_clients.GoogleAdsApiClient(path_to_config=config_path)

  @pytest.fixture
  def failing_api_client(self, mocker, config_path):
    mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
    mocker.patch(
      f'google.ads.googleads.{api_clients.GOOGLE_ADS_API_VERSION}'
      '.services.services.google_ads_service.GoogleAdsServiceClient'
      '.search_stream',
      side_effect=[
        google_exceptions.InternalServerError('test'),
        google_exceptions.InternalServerError('test'),
        google_exceptions.InternalServerError('test'),
      ],
    )
    api_client = api_clients.GoogleAdsApiClient(path_to_config=config_path)
    api_client.get_response.retry.wait = tenacity.wait_none()
    return api_client

  @pytest.fixture
  def fake_report_fetcher(self, mocker, test_client, fake_response):
    mocker.patch(
      'gaarf.api_clients.GoogleAdsApiClient.get_response',
      return_value=fake_response,
    )
    return report_fetcher.AdsReportFetcher(test_client)

  def test_parse_ads_response_returns_success(
    self, fake_report_fetcher, caplog
  ):
    caplog.set_level(logging.DEBUG)
    query_specification = query_editor.QuerySpecification(_QUERY).generate()
    parser = parsers.GoogleAdsRowParser(query_specification)
    customer_id = 1
    results = fake_report_fetcher._parse_ads_response(
      query_specification=query_specification,
      parser=parser,
      customer_id=customer_id,
    )

    assert results == _EXPECTED_RESULTS
    assert 'Getting response for query None for customer_id 1' in (caplog.text)

  @pytest.mark.parametrize(
    'strategy',
    [
      'BATCH',
      'BATCH_PROTOBUF',
    ],
  )
  def test_parse_ads_response_in_batches_generates_warning_messages(
    self, fake_report_fetcher, strategy, caplog
  ):
    query_specification = query_editor.QuerySpecification(_QUERY).generate()
    parser = parsers.GoogleAdsRowParser(query_specification)
    customer_id = 1
    optimize_strategy = report_fetcher.OptimizeStrategy[strategy]
    fake_report_fetcher._parse_ads_response(
      query_specification=query_specification,
      parser=parser,
      customer_id=customer_id,
      optimize_strategy=optimize_strategy,
    )

    assert 'Running gaarf in an optimized mode' in caplog.text
    assert f'Optimize strategy is {optimize_strategy.name}' in caplog.text

  def test_fetch_returns_correct_report(self, fake_report_fetcher):
    fetched_report = fake_report_fetcher.fetch(
      query_specification=_QUERY, customer_ids=[1]
    )
    expected_report = report.GaarfReport(
      results=_EXPECTED_RESULTS, column_names=['customer_id']
    )

    assert fetched_report == expected_report

  def test_fetch_raises_gaarf_exception(
    self, mocker, fake_report_fetcher, caplog
  ):
    mocker.patch(
      'gaarf.report_fetcher.AdsReportFetcher._parse_ads_response',
      side_effect=[
        googleads_exceptions.GoogleAdsException(
          error='test-error',
          call='test-call',
          failure=FakeGoogleAdsFailure(
            errors=[FakeGoogleAdsFailureMessage(message='test-failure')]
          ),
          request_id='test-request-id',
        )
      ],
    )
    with pytest.raises(googleads_exceptions.GoogleAdsException):
      fake_report_fetcher.fetch(query_specification=_QUERY, customer_ids=[1])
    assert (
      'Cannot execute query None for 1 due to the following error: test-failure'
    ) in caplog.text

  def test_parse_ads_response_sequentially_returns_success(
    self, fake_report_fetcher, fake_response
  ):
    query_specification = query_editor.QuerySpecification(_QUERY).generate()
    parser = parsers.GoogleAdsRowParser(query_specification)
    customer_id = 1
    results = fake_report_fetcher._parse_ads_response_sequentially(
      response=fake_response,
      query_specification=query_specification,
      parser=parser,
      customer_id=customer_id,
    )

    assert results == _EXPECTED_RESULTS

  def test_parse_ads_response_in_batches_returns_success(
    self, fake_report_fetcher, fake_response
  ):
    query_specification = query_editor.QuerySpecification(_QUERY).generate()
    parser = parsers.GoogleAdsRowParser(query_specification)
    customer_id = 1
    results = fake_report_fetcher._parse_ads_response_in_batches(
      response=fake_response,
      query_specification=query_specification,
      parser=parser,
      customer_id=customer_id,
    )

    assert_sequence_content_the_same(results, _EXPECTED_RESULTS)

  def test_parse_ads_response_raises_internal_server_error_after_3_failed_attemps(  # noqa: E501
    self, failing_api_client, caplog
  ):
    fetcher = report_fetcher.AdsReportFetcher(failing_api_client)
    query_specification = query_editor.QuerySpecification(_QUERY).generate()
    parser = parsers.GoogleAdsRowParser(query_specification)
    customer_id = 1
    with pytest.raises(google_exceptions.InternalServerError):
      fetcher._parse_ads_response(
        query_specification=query_specification,
        parser=parser,
        customer_id=customer_id,
      )
    assert (
      'Cannot fetch data from API for query '
      f'"{query_specification.query_title}" 3 times'
    ) in caplog.text


def assert_sequence_content_the_same(
  results: list[list[parsers.GoogleAdsRowElement]],
  other_results: list[list[parsers.GoogleAdsRowElement]],
) -> bool:
  return sorted(itertools.chain.from_iterable(results)) == sorted(
    itertools.chain.from_iterable(other_results)
  )
