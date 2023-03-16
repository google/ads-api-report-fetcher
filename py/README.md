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

#### Versions of the library

*  `google-ads-api-report-fetcher[sqlalchemy]` - version with SQLalchemy support
* `google-ads-api-report-fetcher[simulator]` - version with support for [simulating
    query results](../docs/simulating-data-with-gaarf.md) instead of calling Google Ads API.
* `google-ads-api-report-fetcher[full]` - full version

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
from gaarf.io import reader, writer

# initialize Google Ads API client
client = GoogleAdsApiClient(path_to_config="google-ads.yaml", version="v12")

customer_ids = ['1', '2']

# Fetch report and store results in a variable
# initialize report fetcher to get reports
report_fetcher = AdsReportFetcher(client, customer_ids)

# create query text
query_text = "SELECT campaign.id AS campaign_id FROM campaign"

# Execute query and store campaigns variable
campaigns = report_fetcher.fetch(query_text)

# iterate over report
unique_campaigns = set([row.campaign_id for row in campaigns])

# convert `campaigns` to common data structures
campaigns_list = campaigns.to_list()
campaigns_df = campaigns.to_pandas()

# Execute query from file and save results to CSV
# initialize query_executor to fetch report and store them in local/remote storage
query_executor = AdsQueryExecutor(client)

# initialize writer
csv_writer = writer.CsvWriter(destination_folder="/tmp")
reader_client = reader.FileReader()

# execute query and save to csv
query_executor.execute(
    query_text=query_text,
    query_name="campaign",
    customer_ids=customer_ids,
    write_client=csv_writer)

# execute query from file and save to csv
query_path="path/to/query.sql"
query_executor.execute(
    query_text=reader_client.read(query_path),
    query_name=query_path,
    customer_ids=customer_ids,
    write_client=csv_writer)
```

## Python specific command line flags

* `--optimize-performance` - accepts one of the following values:
    * `NONE` - no optimizations are done
    * `PROTOBUF` - convert Google Ads API response to protobuf before parsing
        (speeds up query execution 5x times but forces conversion of ENUMs to integers instead of strings)
    * `BATCH` -  converts all response of Ads API to a list and then parses its content in parallel
    * `BATCH_PROTOBUF` - combines `BATCH` and `PROTOBUF` approaches.

## Disclaimer
This is not an officially supported Google product.
