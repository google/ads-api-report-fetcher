# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module for installing gaarf as a package."""

from __future__ import annotations

import itertools
import pathlib

import setuptools

HERE = pathlib.Path(__file__).parent

README = (HERE / 'README.md').read_text()

EXTRAS_REQUIRE = {
  'pandas': [
    'pandas>=1.3.4',
  ],
  'sqlalchemy': [
    'sqlalchemy',
  ],
  'simulator': [
    'Faker',
  ],
  'sheets': [
    'gspread',
  ],
  'bq': [
    'google-cloud-bigquery',
    'pandas>=1.3.4',
    'smart_open[gcs]',
  ],
}
EXTRAS_REQUIRE['full'] = list(set(itertools.chain(*EXTRAS_REQUIRE.values())))

setuptools.setup(
  name='google-ads-api-report-fetcher',
  version='1.14.1',
  python_requires='>3.8',
  description=(
    'Library for fetching reports from Google Ads API '
    'and saving them locally & remotely.'
  ),
  long_description=README,
  long_description_content_type='text/markdown',
  url='https://github.com/google/ads-api-reports-fetcher',
  author='Google Inc. (gTech gPS CSE team)',
  author_email='no-reply@google.com',
  license='Apache 2.0',
  classifiers=[
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3.12',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Operating System :: OS Independent',
    'License :: OSI Approved :: Apache Software License',
  ],
  packages=setuptools.find_packages(),
  install_requires=[
    'google-ads>=24.1.0',
    'smart_open',
    'jinja2',
    'python-dateutil',
    'typing-extensions',
    'rich',
    'tenacity',
  ],
  extras_require=EXTRAS_REQUIRE,
  setup_requires=[
    'pytest-runner',
  ],
  tests_requires=[
    'pytest',
    'pytest-mock',
  ],
  entry_points={
    'console_scripts': [
      'gaarf=gaarf.cli.gaarf:main',
      'gaarf-py=gaarf.cli.gaarf:main',
      'gaarf-bq=gaarf.cli.bq:main',
      'gaarf-py-bq=gaarf.cli.bq:main',
      'gaarf-simulator=gaarf.cli.simulator:main',
      'gaarf-sql=gaarf.cli.sql:main',
    ]
  },
)
