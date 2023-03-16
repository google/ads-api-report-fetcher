# Simulating data with gaarf

> This functionality is currently supported only in Python version

Sometimes we need to generate some data for prototyping without actually calling
Google Ads API.

There's `gaarf-simulator` CLI tool installed with `google-ads-api-report-fetcher[simulator]`
which allows to simulate data based on provided queries.

`gaarf-simulator` can be used both with `gaarf_config.yaml` and with a set of
command-line flags:


1. Simulate data based on provided queries and output results to console:

```
gaarf-simulator path/to/queries
```

2. Simulate data based on `gaarf_config.yaml` (takes `api_version` and `writer_options` from there).

```
gaarf-simulator path/to/queries \
    -c=/path/to/gaarf_config.yaml \
    -s=/path/to/simulator_specification.yaml
```

3. Simulate data based on provided queries and save results to BigQuery:

```
gaarf-simulator path/to/queries --output=bq \
    --bq.project=<project-name> --bq.dataset=<dataset-name>
```

## Creating simulator specification

Simulator specification is a yaml file which can be used to fine-tune the
simulation results.

It expects the some or all the following columns:
*  `api_version` - which version of Google Ads API to use to perform the
    simulation (by default version `v12` is used)
* `n_rows` - how many rows of data to simulate (by default 1000)
* `days_ago` - if query has any `date` it what it what is the maximum
    lookback window from today (by default `-7d` - last 7 days)
* `string_length` - if any string values are to be generated what is the
    number of characters in such a string (by default 3)
* `allowed_enums` - if a field in the query is Enum should we take a subset
    of provided enums? Keys in `allowed_enums` section should be full names
    of fields/segments/metrics in [Google Ads API Reports](https://developers.google.com/google-ads/api/fields/v12/overview)
* `replacements` - should any particular field/segments/metric be replaced
    with one of provided values? Keys in `replacements` section should be full names
    of fields/segments/metrics in [Google Ads API Reports](https://developers.google.com/google-ads/api/fields/v12/overview)

Check an example of `simulator_specification.yaml` below:

```
api_version: v12
n_rows: 1000
days_ago: -7d
string_length: 3
allowed_enums:
  ad_group_ad_asset_view.field_type:
    - MEDIA_BUNDLE
    - LONG_HEADLINE
    - MARKETING_IMAGE
  campaign.ad_advertising_channel_sub_type:
    - APP_CAMPAIGN
  segments.ad_network_type:
    - CONTENT
    - YOUTUBE
  segments.conversion_source:
    - GOOGLE_PLAY
  segments.conversion_type:
    - DOWNLOAD
    - PURCHASE
replacements:
  asset.youtube_video_asset.youtube_video_id:
    - 1332
```
