-- Comment
# Comment
// Comment

SELECT
	customer.id, --customer_id
	campaign.type AS campaign_type, campaign.id:nested AS campaign,
	ad_group.id~1 AS ad_group,
	ad_group_ad.id->asset AS ad, 
from ad_group_ad 
