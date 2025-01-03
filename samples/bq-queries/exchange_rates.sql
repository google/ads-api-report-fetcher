-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

/**
  A script that you can use to get currency exchange rates in your
  BigQuery scripts to join with.
  It creates a table 'currency_rates' with columns:
    currency_from and currency_to containing currency code pairs (e.g. USD, AED, ILS)
    rate with conversion rate of 'currency_from' to 'currency_to'.

  The table is built on a public Google Spreadsheet with actual data.
  In that Spreadsheet conversion rates are fetched with GOOGLEFINANCE function regualrly.

  Feel free to place the table where it works for you.
  For instance, instead of using `gaarf.currency_rates` you can incorporate using
  a macro for the target dataset to place the table together with other tables:
  `{dst_dataset}.currency_rates_external`
 */
CREATE OR REPLACE EXTERNAL TABLE gaarf.currency_rates_external (
  currency_from STRING,
  currency_to STRING,
  rate STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  skip_leading_rows = 1,
  sheet_range = "TABLE",
  uris = ["https://docs.google.com/spreadsheets/d/1Up8zK3JhoE2zI1n0koi-LQcm6J7-o_slBht3SgVkJYA/edit?usp=sharing"],
  description = "External table for current currency exchange rates generated by Google Finance."
);

-- Create a view that casts the rate to NUMERIC
CREATE OR REPLACE VIEW gaarf.currency_rates AS
SELECT
  currency_from,
  currency_to,
  IFNULL(CAST(rate AS NUMERIC), 1) AS rate
FROM gaarf.currency_rates_external;
