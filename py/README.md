# Ads API Reports Fetcher

## Overview

Ads API Reports Fetcher simplifies running [Google Ads API Reports](https://developers.google.com/google-ads/api/fields/v9/overview)
by separating logic of writing [GAQL](https://developers.google.com/google-ads/api/docs/query/overview)-like query from executing it and saving results.\
The library allows you to define GAQL query alonside aliases and custom extractors and specify where the results of such query should be stored. You can find and example queries in `examples` folder. Based on this query the library fill extract the correct GAQL query, automatically extract all necessary fields from returned `GoogleAdsRow` object and transform them into the structure suitable for writing data.


## Getting started

1. create virtual enviroment

```
python3 -m venv ads-api-fetcher
source ads-api-fetcher/bin/activate
pip install google-ads-api-report-fetcher
```
2. authenticate google ads to create `google-ads.yaml` file

    2.1. Create `google-ads.yaml` file in your home directory with the following content
    (or copy from `configs` folder):

    ```
    developer_token:
    client_id:
    client_secret:
    refresh_token:
    login_customer_id:
    client_customer_id:
    use_proto_plus: True
    ```
    2.2. [Get Google Ads Developer Token](https://developers.google.com/google-ads/api/docs/first-call/dev-token). Add developer token id to `google-ads.yaml` file.

    2.3. [Generate OAuth2 credentials for **desktop application**](https://developers.google.com/adwords/api/docs/guides/authentication#generate_oauth2_credentials)
    * Click the download icon next to the credentials that you just created and save file to your computer
    *  Add client_id and client_secret value to `google-ads.yaml` file

    2.4. Download python source file to perform desktop authentication

    ```
    curl -0 https://raw.githubusercontent.com/googleads/google-ads-python/868bf36689f1ca4310bdead9c46eed61b8ad1d11/examples/authentication/authenticate_in_desktop_application.py
    ```

    2.5. Run desktop authentication with downloaded credentials file:
    ```
    python authenticate_in_desktop_application.py --client_secrets_path=/path/to/secrets.json
    ```
    * Copy generated refresh token and add it to `google-ads.yaml` file.

    2.6. [Enable Google Ads API in your project](https://developers.google.com/google-ads/api/docs/first-call/oauth-cloud-project#enable_the_in_your_project)

    2.7. Add login_customer_id and client_customer_id (MMC under which Developer token was generated) to `google-ads.yaml`. **ID should be in 11111111 format, do not add dashes as separator**.


3. install library

```
pip install google-ads-api-report-fetcher
```

Two commands will be available for using in terminal:

* `gaarf`  - to get data from Ads API based on provided query
   and a set of parameters
* `gaarf-process` - to execute any post-processing queries based on
   results of `fetch-reports` command.


4. Specify enviromental variables

```
export ACCOUNT_ID=
export BQ_PROJECT=
export BQ_DATASET=
export START_DATE=
export END_DATE=
```

`START_DATE` and `END_DATE` should be specified in `YYYY-MM-DD` format (i.e. 2022-01-01).
`CUSTOMER_ID` should be specifed in `1234567890` format (no dashes between digits).

5. Run `gaarf` command to fetch Google Ads data and store them in BigQuery

```
gaarf path/to/sql/google_ads_queries/*.sql \
    --account=$ACCOUNT_ID \
    --output=bq \
    --bq.project=$BQ_PROJECT \
    --bq.dataset=$BQ_DATASET \
    --sql.start_date=$START_DATE \
    --sql.end_date=$END_DATE \
    --ads-config=path/to/google-ads.yaml
```

6. Run `gaarf-postprocess` command to prepare tables in BigQuery based on data
fetched by `gaarf` command.

```
gaarf-postprocess path/to/bq_queries/*.sql \
    --bq.project=$BQ_PROJECT \
    --bq.dataset=$BQ_DATASET \
```

## Disclaimer
This is not an officially supported Google product.

Copyright 2022 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.

