# Using gaarf in Airflow


If you want to use Apache Airflow to run any gaarf-based projects you can use
`airflow-google-ads-api-report-fetcher` package.

Install it with `pip install airflow-google-ads-api-report-fetcher` -
it will make  `airflow_gaarf` library available.

> Install the latest development version with `pip install -e git+https://github.com/google/ads-api-report-fetcher.git#egg=airflow-google-ads-api-report-fetcher\&subdirectory=py/airflow_gaarf`

The library comes with two operators - `GaarfOperator` and `GaarfBqOperator` which can
be used to simplify executing `google_ads_queries` and `bq_queries` respectively.

## Setup
### Connections

Template pipeline expects two type of connections - go to *Admin - Connections*,
add new connection (type *Generic*) and in *Extra* add the values specified below:

* `google_ads_default`

		{"google_ads_client":
			{"developer_token": "",
			"client_id": "",
			"client_secret": "",
			"refresh_token": "",
			"login_customer_id": "",
			"client_customer_id": "",
			"use_proto_plus": "true"
			}
		}

* `gcp_conn_id`

	  {"cloud":
			{"project_id": "your-project"}
	  }


### Examples

Once the above connections were setup you may proceed to configuring DAG.
`examples` folder contains several DAGs you might find useful:

* `01_gaarf_console_reader_console_writer.py` - simple DAG which consist of a single `GaarfOperator` what fetches data from an inline query and outputs results to the console.
* `02_gaarf_file_reader_csv_writer.py` - DAG that reads query from a file (can be local or remote storage) and save results to CSV.
* `03_gaarf_read_file_write_to_sqlalchemy.py` - DAG that reads query from a file (can be local or remote storage) and save results to Postgres via SQLAlchemy connection.
* `04_gaarf_read_solution_directory_and_config` - DAG that reads queries and a config from a directory with queries and for reach query builds its own task.

