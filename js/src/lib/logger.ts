
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

/** Default log level (usualy one of 'info' or 'debug') */
export const LOG_LEVEL = argv.logLevel || process.env.LOG_LEVEL ||
    (process.env.NODE_ENV === 'production' ? 'info' : 'debug');

const colors = {
  error: 'red',
  warn: 'yellow',
  info: 'green',
  http: 'magenta',
  debug: 'white',
}

winston.addColors(colors);

const transports: winston.transport[] = [];
transports.push(new winston.transports.Console({
  format: format.combine(
      format.colorize({all: true}),
      format.printf(
          (info) => `${info.timestamp} ${info.level}: ${info.message}`,
          ),
      )
}));

const logger = winston.createLogger({
  level: LOG_LEVEL,  // NOTE: we use same log level for all transports
  format: format.combine(
      format.timestamp({format: 'YYYY-MM-DD HH:mm:ss:ms'}),
      // format to add 'component' meta value into log message (prepending
      // '[$component] ')
      winston.format((info: winston.Logform.TransformableInfo, opts?: any) => {
        if (info.component && info.message &&
            !info.message.startsWith(`[${info.component}]`)) {
          info.message = `[${info.component}] ${info.message}`;
        }
        return info;
      })()),
  transports
});
export default logger;
