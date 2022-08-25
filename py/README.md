# Google Ads API Report Fetcher (gaarf)

Python version of Google Ads API Report Fetcher tool a.k.a. `gaarf`.
Please see the full documentation in the root [README](https://github.com/google/ads-api-report-fetcher/blob/main/README.md).

## Getting started

### Prerequisites

* Python 3.8+
* pip installed
* Google Ads API enabled
* `google-ads.yaml` file. Learn how to create one [here](../docs/how-to-authenticate-ads-api.md).

### Installation and running

1. create virtual environment and install the tool

```
python3 -m venv gaarf
source gaarf/bin/activate
pip install google-ads-api-report-fetcher
```
> install the latest development version with `pip install -e git+https://github.com/google/ads-api-report-fetcher.git#egg=google-ads-api-report-fetcher\&subdirectory=py`
2.  Run the tool with `gaarf` command:

```shell
gaarf <queries> [options]
```

Documentation on available options see in the root [README.md](../README.md).


## Using as a library

Once `google-ads-api-report-fetcher` is installed you can use it as a library.


```python
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.query_executor import AdsReportFetcher, AdsQueryExecutor
from gaarf.io import writer

# initialize Google Ads API client
client = GoogleAdsApiClient(path_to_config="google-ads.yaml", version="v10")

customer_ids = ['1', '2']

# Fetch report and store results in a variable
# initialize report fetcher to get reports
report_fetcher = AdsReportFetcher(client, customer_ids)

# create query text
query_text = "SELECT campaign.id AS campaign_id FROM campaign"

# Execute query and store campaigns variable
campaigns = report_fetcher.fetch(query_text)

# convert `campaigns` to common data structures
campaigns_list = campaigns.to_list()
campaigns_df = campaigns.to_pandas()

# Fetch report and save it to CSV
# initialize query_executor to fetch report and store them in local/remote storage
query_executor = AdsQueryExecutor(client)

# initialize writer
csv_writer = writer.CsvWriter(".tmp")

# specify path to GAQL query
query_path = "path/to/query.sql"

# execute query from file and save to csv
query_executor.execute(query_path, customer_ids, reader_client, csv_writer)
```


## Disclaimer
This is not an officially supported Google product.

Copyright 2022 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.

