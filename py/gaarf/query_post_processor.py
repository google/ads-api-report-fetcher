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

import logging
from typing import Any, Dict, Optional

from jinja2 import Environment, FileSystemLoader, Template

logger = logging.getLogger(__name__)


class PostProcessorMixin:
  def replace_params_template(
    self, query_text: str, params: Optional[Dict[str, Any]] = None
  ) -> str:
    logger.debug('Original query text:\n%s', query_text)
    if params:
      if templates := params.get('template'):
        query_templates = {
          name: value for name, value in templates.items() if name in query_text
        }
        if query_templates:
          query_text = self.expand_jinja(query_text, query_templates)
          logger.debug('Query text after jinja expansion:\n%s', query_text)
        else:
          query_text = self.expand_jinja(query_text, {})
      else:
        query_text = self.expand_jinja(query_text, {})
      if macros := params.get('macro'):
        query_text = query_text.format(**macros)
        logger.debug('Query text after macro substitution:\n%s', query_text)
    else:
      query_text = self.expand_jinja(query_text, {})
    return query_text

  def expand_jinja(
    self, query_text: str, template_params: Optional[Dict[str, Any]] = None
  ) -> str:
    file_inclusions = ('% include', '% import', '% extend')
    if any(file_inclusion in query_text for file_inclusion in file_inclusions):
      template = Environment(loader=FileSystemLoader('.'))
      query = template.from_string(query_text)
    else:
      query = Template(query_text)
    if not template_params:
      return query.render()
    for key, value in template_params.items():
      if value:
        if isinstance(value, list):
          template_params[key] = value
        elif len(splitted_param := value.split(',')) > 1:
          template_params[key] = splitted_param
        else:
          template_params[key] = value
      else:
        template_params = ''
    return query.render(template_params)
