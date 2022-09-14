from gaarf.api_clients import GoogleAdsApiClient


def test_infer_types():
    client = GoogleAdsApiClient(config_dict={}, version="v11")
    fields = [
        'ad_group_ad.ad.id',
        'ad_group_ad.ad.legacy_responsive_display_ad.long_headline',
        'customer.descriptive_name', 'campaign.id', 'campaign.name',
        'campaign.app_campaign_setting.bidding_strategy_goal_type',
        'segments.date', 'metrics.clicks', 'metrics.ctr',
        'segments.asset_interaction_target.interaction_on_this_asset',
    ]
    output = client.infer_types(fields)
    expected = [int, str, str, int, str, str, str, int, float, bool]
    assert output == expected
