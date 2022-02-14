from setuptools import setup, find_packages

setup(name="ads-api-reports-fetcher",
      version="0.1",
      packages=find_packages(include=["runner", "runner.*"]),
      install_requires=[
          "google-ads==14.1.0", "google-cloud-bigquery==2.26.0",
          "pandas==1.3.4", "pyarrow==6.0.1"
      ],
      setup_requires=["pytest-runner"],
      tests_requires=["pytest"],
      entry_points={
          "console_scripts": [
              "fetch-reports=runner.fetcher:main",
              "post-process-queries=runner.post_processor:main",
          ]
      })
