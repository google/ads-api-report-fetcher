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

import express from 'express';
import {getLogger, ILogger} from 'google-ads-api-report-fetcher';
export {ILogger} from 'google-ads-api-report-fetcher';

export function createLogger(
  req: express.Request,
  projectId: string,
  component: string
): ILogger {
  const logLevel = <string>req.query.log_level || process.env.LOG_LEVEL;
  if (logLevel) {
    process.env.LOG_LEVEL = logLevel;
  }
  const traceHeader = req.header('X-Cloud-Trace-Context');
  if (traceHeader && projectId) {
    const [trace] = traceHeader.split('/');
    process.env.TRACE_ID = `projects/${projectId}/traces/${trace}`;
  }
  const logger = getLogger();
  if (logLevel) {
    logger.level = logLevel;
  }
  // NOTE: here we're setting some environment variables for winston logger in gaarf library
  process.env.LOG_COMPONENT = component;
  process.env.GCP_PROJECT = projectId;
  return logger;
}
