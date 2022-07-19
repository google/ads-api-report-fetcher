import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="airflow-google-ads-api-report-fetcher",
    version="0.0.1",
    description=
    "Library for running google-ads-api-report-fetcher in Apache Airflow.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/google/ads-api-reports-fetcher/py/gaarf_airflow/",
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
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "google-ads-api-report-fetcher",
        "apache-airflow",
    ],
    )
