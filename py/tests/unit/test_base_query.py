from __future__ import annotations

import dataclasses

from gaarf import base_query


class FakeQueryWithoutInit(base_query.BaseQuery):
  query_text = 'SELECT campaign.id FROM campaign'


class FakeQueryWithInit(base_query.BaseQuery):
  query_text = """
        SELECT campaign.id FROM campaign WHERE campaign.id = {campaign_id}
        """

  def __init__(self, campaign_id: int) -> None:
    self.campaign_id = campaign_id


@dataclasses.dataclass
class FakeQueryDataclass(base_query.BaseQuery):
  query_text = """
        SELECT campaign.id FROM campaign WHERE campaign.id = {campaign_id}
        """
  campaign_id: int


class FakeQueryOldStyle(base_query.BaseQuery):
  def __init__(self, campaign_id: int) -> None:
    self.query_text = f"""
            SELECT campaign.id FROM campaign WHERE campaign.id = {campaign_id}
        """


class TestBaseQuery:
  def test_base_query_correct_init(self):
    expected_query_text = 'SELECT campaign.id FROM campaign'
    fake_query = FakeQueryWithoutInit()
    assert str(fake_query) == expected_query_text.strip()

  def test_base_query_correct_init_with_argument(self):
    expected_query_text = """
        SELECT campaign.id FROM campaign WHERE campaign.id = 1
        """
    fake_query = FakeQueryWithInit(campaign_id=1)
    assert str(fake_query) == expected_query_text.strip()

  def test_base_query_correct_init_with_argument_old_style(self):
    expected_query_text = """
        SELECT campaign.id FROM campaign WHERE campaign.id = 1
        """
    fake_query = FakeQueryOldStyle(campaign_id=1)
    assert str(fake_query) == expected_query_text.strip()

  def test_base_query_correct_init_with_argument_dataclass(self):
    expected_query_text = """
        SELECT campaign.id FROM campaign WHERE campaign.id = 1
        """
    fake_query = FakeQueryDataclass(campaign_id=1)
    assert str(fake_query) == expected_query_text.strip()
