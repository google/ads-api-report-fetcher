# Comment

SELECT
	customer.id,
	campaign.type AS campaign_type,
	campaign.id:nested AS campaign,
	ad_group.id~1 AS ad_group 
	ad_group_ad.id->asset AS ad 
FROM ad_group_ad 
