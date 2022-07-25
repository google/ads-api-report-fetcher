# Copyright 2022 Google LLC
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

class BaseQuery:
    def __init__(self, **kwargs):
        raise NotImplementedError

    def __str__(self):
        if hasattr(self, "query_text"):
            return self.query_text
        raise NotImplementedError(
            "attribute self.query_text must be implemented "
            f"in class {self.__class__.__name__}"
        )
