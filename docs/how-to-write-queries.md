# How to write queries

## Table of content

 - [Intro](#intro)
 - [Elements](#elements)
 - [Aliases](#aliases)
 - [Nested Resources](#nested-resources)
 - [Resource Indices](#resource-indices)


## Intro
Google Ads API Report Fetcher uses [GAQL](https://developers.google.com/google-ads/api/docs/query/overview)
syntax with some extended capabilities.


This is how a generic query might look like:

```
SELECT
    ad_group.id,
    ad_group.name
FROM ad_group
```

When running this query and saving the results we get pretty long and unreadable
column names - `ad_group.id` and `ad_group.name`.

Things might be more complicated if you want to extract and save such objects
as unselectable elements, complex messages and resource names.

In order to simplify data extraction and processing when querying data from Ads API
we introduce additional syntax (see an example below):

```
SELECT
    resource.attribute AS column_name_1,
    resource.attribute:nested.resource AS column_name_3
    resource.attribute~1 AS column_name_4
FROM resource
```

## Elements

* Aliases (`AS column_name`)
* Nested resources (`:nested.resource.name`)
* Resource indices (`~position`)
* Functions (`:$func`) - only in Node.js

### Aliases

Alias is used to give a descriptive name to a metric or attribute fetched from
API when saving data. So instead of column name
`campaign.app_campaign_setting.bidding_strategy_goal_type` you may use something
more user friendly, like `bidding_type`.

Aliases are specified using `AS` keyword as shown below:

```
SELECT
    campaign.app_campaign_setting.bidding_strategy_goal_type AS bidding_type
FROM campaign
```


### Nested Resources

Nested resources return many attributes and you want to get a particular one.
One particular example is working with `change_event` - `change_event.new_resource`
consists of various changes made to an entity and looks something like that:

```
new_resource {
    campaign {
        target_cpa {
            target_cpa_micros: 1000000
        }
    }
}
```

In order to extract a particular element (i.e., final value for `target_cpa_micros`)
we use the `:` syntax - `change_event.new_resource:campaign.target_cpa.target_cpas_micros`:

```
SELECT
    change_event.old_resource:campaign.target_cpa.target_cpa_micros AS old_target_cpa,
    change_event.new_resource:campaign.target_cpa.target_cpa_micros AS new_target_cpa
FROM change_event
```

### Resource Indices

Resource indices are used to extract a particular element from data type
*RESOURCE_NAME*. I.e., if we want to get resource name for `campaign_audience_view.resource_name`
and save it somewhere, the saved result will contain a string *customers/{customer_id}/campaignAudienceViews/{campaign_id}~{criterion_id}*. 
Usually we want to get only the last element from (`criterion_id`) and
it can be extracted with `~N` syntax  where *N* is a position of an element you want to extract
(indexing is starting from 0).

If the resource you're selecting looks like this `customers/111/campaignAudienceViews/222~333`
you can specify `campaign_audience_view.resource_name~1` to extract the second element (`333`).
If you specify `campaign_audience_view.resource_name~0` you'll get '222' (the last resource id before ~).

```
SELECT
    campaign_audience_view.resource_name~1 AS criterion_id
FROM campaign_audience_view
```

