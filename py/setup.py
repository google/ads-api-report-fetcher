import pathlib
from setuptools import setup, find_packages
from itertools import chain

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

EXTRAS_REQUIRE = {
    "sqlalchemy": ["sqlalchemy"],
    "simulator": ["Faker"],
    "sheets": ["gspread"]
}
EXTRAS_REQUIRE['full'] = list(set(chain(*EXTRAS_REQUIRE.values())))

setup(
    name="google-ads-api-report-fetcher",
    version="1.12.2",
    python_requires=">3.8",
    description=
    "Library for fetching reports from Google Ads API and saving them locally / BigQuery.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/google/ads-api-reports-fetcher",
    author="Google Inc. (gTech gPS CSE team)",
    author_email="no-reply@google.com",
    license="Apache 2.0",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License"
    ],
    packages=find_packages(),
    install_requires=[
        "google-ads>=22.1.0", "google-cloud-bigquery", "pandas>=1.3.4",
        "pyarrow>=14.0.1", "smart_open[all]", "jinja2", "python-dateutil",
        "rich", "tenacity"
    ],
    extras_require=EXTRAS_REQUIRE,
    setup_requires=["pytest-runner"],
    tests_requires=["pytest"],
    entry_points={
        "console_scripts": [
            "gaarf=gaarf.cli.gaarf:main",
            "gaarf-py=gaarf.cli.gaarf:main",
            "gaarf-bq=gaarf.cli.bq:main",
            "gaarf-py-bq=gaarf.cli.bq:main",
            "gaarf-simulator=gaarf.cli.simulator:main",
            "gaarf-sql=gaarf.cli.sql:main",
        ]
    })
