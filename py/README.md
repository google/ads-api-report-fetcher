# Google Ads API Report Fetcher (gaarf)

[![](https://img.shields.io/pypi/pyversions/google-ads-api-report-fetcher.svg)](https://pypi.org/project/google-ads-api-report-fetcher/)

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

*  `google-ads-api-report-fetcher[bq]` - version with BigQuery support
*  `google-ads-api-report-fetcher[pandas]` - version with Pandas support
*  `google-ads-api-report-fetcher[sqlalchemy]` - version with SQLalchemy support
* `google-ads-api-report-fetcher[simulator]` - version with support for [simulating
    query results](../docs/simulating-data-with-gaarf.md) instead of calling Google Ads API.
*  `google-ads-api-report-fetcher[sheets]` - version with Google Sheets support
* `google-ads-api-report-fetcher[full]` - full version

2.  Run the tool with `gaarf` command:

```shell
gaarf <queries> [options]
```

Documentation on available options see in the root [README.md](../README.md).


## Using as a library

Once `google-ads-api-report-fetcher` is installed you can use it as a library.


### Initialize `GoogleAdsApiClient` to connect to Google Ads API

`GoogleAdsApiClient` is responsible for connecting to Google Ads API and provides several methods for authentication.

```python
from gaarf import GoogleAdsApiClient


# initialize from local file
client = GoogleAdsApiClient(path_to_config="google-ads.yaml")

# initialize from remote file
client = GoogleAdsApiClient(path_to_config="gs://<PROJECT-ID>/google-ads.yaml")

# initialize from dictionary
google_ads_config_dict = {
    "developer_token": "",
    "client_id": "",
    "client_secret": "",
    "refresh_token": "",
    "client_customer_id": "",
    "use_proto_plus": True
}
client = GoogleAdsApiClient(config_dict=google_ads_config_dict)
```

### initialize `AdsReportFetcher` to get reports

```python
from gaarf.report_fetcher import AdsReportFetcher

report_fetcher = AdsReportFetcher(client)

# create query text
query_text = "SELECT campaign.id AS campaign_id FROM campaign"

# Execute query and store `campaigns` variable
# specify customer_ids explicitly
customer_ids = ['1', '2']
# or perform mcc expansion for mcc 1234567890
customer_ids = report_fetcher.expand_mcc('1234567890')
campaigns = report_fetcher.fetch(query_text, customer_ids)

# perform mcc expansion when calling `fetch` method
campaigns = report_fetcher.fetch(query_text, '1234567890', auto_expand=True)
```

#### Use macros in your queries

```python
parametrized_query_text = """
    SELECT
        campaign.id AS campaign_id
    FROM campaign
    WHERE campaign.status = '{status}'
    """
active_campaigns = report_fetcher.fetch(parametrized_query_text, customer_ids,
                                        {"macro": {
                                            "status": "ENABLED"
                                        }})
```

#### Define queries

There are three ways how you can define a query:
* in a variable
* in a file
* in a class (useful when you have complex parametrization and validation)

```python
from gaarf.base_query import BaseQuery
from gaarf.io import reader


# 1. define query as a string an save in a variable
query_string = "SELECT campaign.id FROM campaign"

# 2. define path to a query file and read from it
# path can be local
query_path = "path/to/query.sql"
# or remote
query_path = "gs://PROJECT_ID/path/to/query.sql"

# Instantiate reader
reader_client = reader.FileReader()
# And read from the path
query = reader_client.read(query_path)

# 3. define query as a class

# New style
class Campaigns(BaseQuery):
    query_text  = """
        SELECT
            campaign.id
        FROM campaign
        WHERE campaign.status = {status}
        """

    def __init__(self, status: str = "ENABLED") -> None:
        self.status = status

# Dataclass style
from dataclasses import dataclass

@dataclass
class Campaigns(BaseQuery):
    query_text  = """
        SELECT
            campaign.id
        FROM campaign
        WHERE campaign.status = {status}
        """
    status: str = "ENABLED"

# Old style
class Campaigns(BaseQuery):
    def __init__(self, status: str = "ENABLED"):
        self.query_text = f"""
        SELECT
            campaign.id
        FROM campaign
        WHERE campaign.status = {status}
        """

active_campaigns = report_fetcher.fetch(Campaigns())
inactive_campaigns = report_fetcher.fetch(Campaigns("INACTIVE"))
```

#### Iteration and slicing

`AdsReportFetcher.fetch` method returns an instance of `GaarfReport` object which you can use to perform simple iteration.

```python
query_text = """
    SELECT
        campaign.id AS campaign_id,
        campaign.name AS campaign_name,
        metrics.clicks AS clicks
    FROM campaign
    WHERE segments.date DURING LAST_7_DAYS
    """
campaigns = report_fetcher.fetch(query_text, '1234567890', auto_expand=True)

# iterate over each row of `campaigns` report
for row in campaigns:
    # Get element as an attribute
    print(row.campaign_id)

    # Get element as a slice
    print(row["campaign_name"])

    # Get element as an index (will print number of clicks)
    print(row[2])

    # Create new column
    row["new_campaign_id"] = row["campaign_id"] + 1
```


You can easily slice the report

```python
# Create new reports by selecting one or more columns
campaign_only_report = campaigns["campaign_name"]
campaign_name_clicks_report = campaigns[["campaign_name", "clicks"]]

# Get subset of the report
# Get first row only
first_campaign_row = campaigns[0]
# Get first ten rows from the report
first_10_rows_from_campaigns = campaigns[0:10]
```

#### Convert report

`GaarfReport` can be easily converted to common data structures:

```python
# convert `campaigns` to list of lists
campaigns_list = campaigns.to_list()

# convert `campaigns` to flatten list
campaigns_list = campaigns.to_list(row_type="scalar")

# convert `campaigns` column campaign_id to list
campaigns_list = campaigns["campaign_id"].to_list()

# convert `campaigns` column campaign_id to list with unique values
campaigns_list = campaigns["campaign_id"].to_list(distinct=True)

# convert `campaigns` to list of dictionaries
# each dictionary maps report column to its value, i.e.
# {"campaign_name": "test_campaign", "campaign_id": 1, "clicks": 10}
campaigns_list = campaigns.to_list(row_type="dict")

# convert `campaigns` to pandas DataFrame
campaigns_df = campaigns.to_pandas()

# convert `campaigns` to dictionary
# map campaign_id to campaign_name one-to-one
campaigns_df = campaigns.to_dict(
    key_column="campaign_id",
    value_column="campaign_name",
    value_column_output="scalar",
    )

# convert `campaigns` to dictionary
# map campaign_id to campaign_name one-to-many
campaigns_df = campaigns.to_dict(
    key_column="campaign_id",
    value_column="campaign_name",
    value_column_output="list",
    )
```

#### Build report

`GaarfReport` can be easily built from pandas data frame:

```
import pandas as pd

df = pd.DataFrame(data=[[1]], columns=["one"])
report = GaarfReport.from_pandas(df)
```

#### Save report

`GaarfReport` can be easily saved to local or remote storage:

```python
from gaarf.io import writers

# initialize CSV writer
csv_writer = writers.csv_writer.CsvWriter(destination_folder="/tmp")

# initialize BigQuery writer
bq_writer = writers.bigquery_writer.BigQueryWriter(
    project="", dataset="", location="")

# initialize SQLAlchemy writer
sqlalchemy_writer = writers.sqlalchemy_writer.SqlAlchemyWriter(
    connection_string="")

# initialize Console writer
console_writer = writers.console_writer.ConsoleWriter(page_size=10)

# initialize Json writer
json_writer = writers.json_writer.JsonWriter(destination_folder="/tmp")

# initialize Google Sheets writer
sheet_writer = writers.sheets_writer.SheetWriter(
    share_with="you@email.com",
    credential_files="path/to/credentials.json"
    )


# save report using one of the writers
csv_writer.write(campaigns, destination="my_file_name")
bq_writer.write(campaigns, destination="my_table_name")
sqlalchemy_writer.write(campaigns, destination="my_table_name")
json_writer.write(campaigns, destination="my_table_name")
sheet_writer.write(campaigns, destination="my_table_name")
```

### Combine fetching and saving with `AdsQueryExecutor`

If your job is to execute query and write it to local/remote storage you can use `AdsQueryExecutor` to do it easily.
> When reading query from file `AdsQueryExecutor` will use query file name as a name for output file/table.
```python
from gaarf.io import reader, writers
from gaarf.executors import AdsQueryExecutor


# initialize query_executor to fetch report and store them in local/remote storage
query_executor = AdsQueryExecutor(client)

# initialize writer
csv_writer = writers.csv_writer.CsvWriter(destination_folder="/tmp")
reader_client = reader.FileReader()

query_text = """
    SELECT
        campaign.id AS campaign_id,
        campaign.name AS campaign_name,
        metrics.clicks AS clicks
    FROM campaign
    WHERE segments.date DURING LAST_7_DAYS
    """

# execute query and save results to `/tmp/campaign.csv`
query_executor.execute(
    query_text=query_text,
    query_name="campaign",
    customer_ids=customer_ids,
    write_client=csv_writer)

# execute query from file and save to results to `/tmp/query.csv`
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
