from __future__ import annotations

import datetime

import pytest

from gaarf import exceptions, query_editor


@pytest.fixture
def query():
  query = """
-- Comment
# Comment
// Comment

SELECT
    1 AS constant,
    '2023-01-01' AS date,
    '{current_date}' AS current_date,
    metrics.clicks / metrics.impressions AS ctr,
    customer.id, --customer_id
    campaign.bidding_strategy_type AS campaign_type, campaign.id:nested AS campaign,
    ad_group.id~1 AS ad_group,
    ad_group_ad.ad.id->asset AS ad,
    metrics.cost_micros * 1e6 AS cost,
    {% if selective == "true" %}
        campaign.selective_optimization AS selective_optimization,
    {% endif %}
from ad_group_ad;
"""
  return query


@pytest.fixture
def builtin_query():
  return 'SELECT * FROM builtin.ocid_mapping'


def test_builtin_query_returns_valid_specification(builtin_query):
  spec = query_editor.QuerySpecification(
    title='/tmp/ocid_mapping.sql', text=builtin_query, args=None
  ).generate()
  assert spec.is_builtin_query
  assert spec.query_title == 'ocid_mapping'
  assert spec.resource_name == 'builtin.ocid_mapping'
  assert not spec.column_names


@pytest.fixture
def query_specification(query):
  return query_editor.QuerySpecification(
    title='sample_query', text=query, args=None
  )


@pytest.fixture
def query_specification_template(query):
  return query_editor.QuerySpecification(
    title='templated_query',
    text=query,
    args={'template': {'selective': 'true'}},
  )


@pytest.fixture
def sample_query(query_specification):
  return query_specification.generate()


@pytest.fixture
def templated_query(query_specification_template):
  return query_specification_template.generate()


class TestRegularQuery:
  def test_correct_title(self, sample_query):
    assert sample_query.query_title == 'sample_query'

  def test_extract_correct_fields(self, sample_query):
    assert sample_query.fields == [
      'customer.id',
      'campaign.bidding_strategy_type',
      'campaign.id',
      'ad_group.id',
      'ad_group_ad.ad.id',
    ]

  def test_extract_correct_aliases(self, sample_query):
    assert sample_query.column_names == [
      'constant',
      'date',
      'current_date',
      'ctr',
      'customer_id',
      'campaign_type',
      'campaign',
      'ad_group',
      'ad',
      'cost',
    ]

  def test_extract_correct_text(self, sample_query):
    assert (
      sample_query.query_text.lower()
      == 'select customer.id, campaign.bidding_strategy_type, campaign.id, ad_group.id, ad_group_ad.ad.id, metrics.clicks, metrics.impressions, metrics.cost_micros from ad_group_ad'
    )

  def test_extract_custom_callers(self, sample_query):
    assert sample_query.customizers == {
      'campaign': {'type': 'nested_field', 'value': 'nested'},
      'ad_group': {'type': 'resource_index', 'value': 1},
      'ad': {'type': 'pointer', 'value': 'asset'},
    }

  def test_extract_query_lines(self, query_specification, sample_query):
    extracted_lines = query_specification.extract_query_lines(
      sample_query.query_text
    )
    assert extracted_lines == [
      'customer.id',
      'campaign.bidding_strategy_type',
      'campaign.id',
      'ad_group.id',
      'ad_group_ad.ad.id',
      'metrics.clicks',
      'metrics.impressions',
      'metrics.cost_micros',
    ]

  def test_extract_correct_resource(self, sample_query):
    assert sample_query.resource_name == 'ad_group_ad'

  def test_is_constant_resource(self, sample_query):
    assert sample_query.is_constant_resource == False

  def test_has_virtual_columns(self, sample_query):
    assert sample_query.virtual_columns == {
      'constant': query_editor.VirtualColumn(type='built-in', value=1),
      'date': query_editor.VirtualColumn(type='built-in', value='2023-01-01'),
      'current_date': query_editor.VirtualColumn(
        type='built-in', value=datetime.date.today().strftime('%Y-%m-%d')
      ),
      'ctr': query_editor.VirtualColumn(
        type='expression',
        value='metrics.clicks / metrics.impressions',
        fields=['metrics.clicks', 'metrics.impressions'],
        substitute_expression='{metrics_clicks} / {metrics_impressions}',
      ),
      'cost': query_editor.VirtualColumn(
        type='expression',
        value='metrics.cost_micros * 1e6',
        fields=['metrics.cost_micros'],
        substitute_expression='{metrics_cost_micros} * 1e6',
      ),
    }

  def test_incorrect_resource_raises_value_error(self):
    query = 'SELECT metrics.clicks FROM ad_groups'
    spec = query_editor.QuerySpecification(
      title='sample_query', text=query, args=None
    )
    with pytest.raises(exceptions.GaarfResourceException):
      spec.generate()

  def test_incorrect_specification_raises_macro_error(self):
    query = "SELECT '${custom_field}', ad_group.id FROM ad_group"
    spec = query_editor.QuerySpecification(
      title='sample_query', text=query, args=None
    )
    with pytest.raises(exceptions.GaarfMacroException):
      spec.generate()

  def test_incorrect_specification_raises_virtual_column_error(self):
    query = 'SELECT 1, ad_group.id AS ad_group_id FROM ad_group'
    spec = query_editor.QuerySpecification(
      title='sample_query', text=query, args=None
    )
    with pytest.raises(exceptions.GaarfVirtualColumnException):
      spec.generate()

  def test_incorrect_field_raises_value_error(self):
    query = 'SELECT metric.impressions, ad_group.id FROM ad_group'
    spec = query_editor.QuerySpecification(
      title='sample_query', text=query, args=None
    )
    with pytest.raises(exceptions.GaarfFieldException):
      spec.generate()


class TestTemplatedQuery:
  def test_extract_correct_fields(self, templated_query):
    assert templated_query.fields == [
      'customer.id',
      'campaign.bidding_strategy_type',
      'campaign.id',
      'ad_group.id',
      'ad_group_ad.ad.id',
      'campaign.selective_optimization',
    ]

  def test_extract_correct_aliases(self, templated_query):
    assert templated_query.column_names == [
      'constant',
      'date',
      'current_date',
      'ctr',
      'customer_id',
      'campaign_type',
      'campaign',
      'ad_group',
      'ad',
      'cost',
      'selective_optimization',
    ]

  def test_extract_correct_text(self, templated_query):
    assert (
      templated_query.query_text.lower()
      == 'select customer.id, campaign.bidding_strategy_type, campaign.id, ad_group.id, ad_group_ad.ad.id, campaign.selective_optimization, metrics.clicks, metrics.impressions, metrics.cost_micros from ad_group_ad'
    )

  def test_extract_query_lines(self, query_specification, templated_query):
    extracted_lines = query_specification.extract_query_lines(
      templated_query.query_text
    )
    assert extracted_lines == [
      'customer.id',
      'campaign.bidding_strategy_type',
      'campaign.id',
      'ad_group.id',
      'ad_group_ad.ad.id',
      'campaign.selective_optimization',
      'metrics.clicks',
      'metrics.impressions',
      'metrics.cost_micros',
    ]
