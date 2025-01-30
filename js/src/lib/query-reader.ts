/*
 Copyright 2025 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import path from 'path';
import chalk from 'chalk';
import {globSync} from 'glob';
import {getFileContent} from './file-utils.js';
import {IQueryReader, InputQuery} from './types.js';
import {getLogger} from './logger.js';

export class FileQueryReader implements IQueryReader {
  scripts: string[];
  logger;

  constructor(scripts: string[] | undefined) {
    this.scripts = [];
    if (scripts && scripts.length) {
      for (let script of scripts) {
        if (script.includes('*') || script.includes('**')) {
          const expanded_files = globSync(script);
          this.scripts.push(...expanded_files);
        } else {
          script = script.trim();
          if (script) this.scripts.push(script);
        }
      }
    }
    this.logger = getLogger();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<InputQuery> {
    for (const script of this.scripts) {
      const queryText = await getFileContent(script);
      this.logger.info(`Processing query from ${chalk.gray(script)}`);
      const scriptName = path.basename(script).split('.sql')[0];
      const item = {name: scriptName, text: queryText};
      yield item;
    }
  }
}

export class ConsoleQueryReader implements IQueryReader {
  scripts: string[];
  logger;

  constructor(scripts: string[] | undefined) {
    this.scripts = scripts || [];
    this.logger = getLogger();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<InputQuery> {
    let i = 0;
    for (let script of this.scripts) {
      i++;
      let scriptName = 'query' + i;
      const match = script.match(/^([\d\w]+):/);
      if (match && match.length > 1) {
        scriptName = match[1];
        script = script.substring(scriptName.length + 1);
      }
      this.logger.info(
        `Processing inline query ${scriptName}:\n ${chalk.gray(script)}`
      );
      const item = {name: scriptName, text: script};
      yield item;
    }
  }
}
