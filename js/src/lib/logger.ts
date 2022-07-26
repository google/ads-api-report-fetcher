
/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import winston from 'winston';
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;

const {format} = winston;

/** Default log level */
export const LOG_LEVEL = argv.loglevel || process.env.LOG_LEVEL ||
    (process.env.NODE_ENV === 'production' ? 'info' : 'verbose');

const colors = {
  error: 'red',
  warn: 'yellow',
  info: 'white',
  verbose: 'gray',
  debug: 'grey',
};

winston.addColors(colors);

function wrap(str: string) {
  return str ? ' [' + str + ']' : '';
}

const transports: winston.transport[] = [];
transports.push(new winston.transports.Console({
  format: format.combine(
      format.colorize({all: true}),
      format.printf(
          (info) => `${info.timestamp}${wrap(info.scriptName)}${wrap(info.customerId)}: ${info.message}`,
          ),
      )
}));

const logger = winston.createLogger({
  level: LOG_LEVEL,  // NOTE: we use same log level for all transports
  format: format.combine(
      format.timestamp({format: 'YYYY-MM-DD HH:mm:ss:ms'}),
      ),
  transports
});
export default logger;
