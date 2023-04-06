/*
 Copyright 2023 Google LLC

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

import {Log, Logging} from '@google-cloud/logging';
import {LogEntry} from '@google-cloud/logging/build/src/entry';
import express from 'express';

export interface ILogger {
  info(message: string, aux?: any): Promise<void>;
  warn(message: string, aux?: any): Promise<void>;
  error(message: string, aux?: any): Promise<void>;
}

export function createLogger(
  req: express.Request,
  projectId: string,
  component: string
): ILogger {
  const logging = new Logging({projectId: projectId});
  const log = logging.log('gaarf');
  const log_method = cloud_log.bind(null, log, req, projectId);
  const logger = {
    info: async (message: string, aux?: any) => {
      return log_method('INFO', message, component, aux);
    },
    warn: async (message: string, aux?: any) => {
      return log_method('WARN', message, component, aux);
    },
    error: async (message: string, aux?: any) => {
      return log_method('ERROR', message, component, aux);
    },
  };
  // NOTE: here we're setting some environment variables for winston logger in gaarf library
  process.env.LOG_COMPONENT = component;
  process.env.GCP_PROJECT = projectId;
  return logger;
}

async function cloud_log(
  log: Log,
  req: express.Request,
  project: string,
  severity: string,
  message: string,
  component: string,
  aux?: any
) {
  const metadata: LogEntry = {
    severity: severity,
    labels: {
      component: component,
    },
    httpRequest: req,
    resource: {
      labels: {
        function_name: component,
      },
      type: 'cloud_function',
    },
  };
  const traceHeader = req.header('X-Cloud-Trace-Context');
  if (traceHeader && project) {
    const [trace] = traceHeader.split('/');
    metadata.trace = `projects/${project}/traces/${trace}`;
    process.env.TRACE_ID = metadata.trace;
  }
  const entry = log.entry(
    metadata,
    aux ? Object.assign(aux || {}, {text: message}) : message
  );
  await log.write(entry);
}
