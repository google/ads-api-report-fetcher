# Copyright 2023 Google LLC
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

from typing import Any, Dict, Optional
import logging
import sqlalchemy
import pandas as pd

from .query_post_processor import PostProcessorMixin

logger = logging.getLogger(__name__)


class SqlAlchemyQueryExecutor(PostProcessorMixin):

    def __init__(self, engine: sqlalchemy.engine.base.Engine) -> None:
        self.engine = engine

    def execute(
            self,
            script_name: str,
            query_text: str,
            params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        query_text = self.replace_params_template(query_text, params)
        with self.engine.begin() as conn:
            conn.connection.executescript(query_text)
        return None
