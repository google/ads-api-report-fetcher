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

import winston from "winston";
import {
  LoggingWinston,
  getDefaultMetadataForTracing,
  LOGGING_TRACE_KEY,
} from "@google-cloud/logging-winston";
const argv = require("yargs/yargs")(process.argv.slice(2)).argv;

const { format } = winston;

/** Default log level */
export const LOG_LEVEL =
  argv.loglevel ||
  process.env.LOG_LEVEL ||
  (process.env.NODE_ENV === "dev" ? "verbose" : "info");

const colors = {
  error: "red",
  warn: "yellow",
  info: "white",
  verbose: "gray",
  debug: "grey",
};

function wrap(str: string) {
  return str ? " [" + str + "]" : "";
}
const formats: winston.Logform.Format[] = [];
if (process.stdout.isTTY) {
  formats.push(format.colorize({ all: true }));
  winston.addColors(colors);
}
formats.push(
  format.printf(
    (info) =>
      `${info.timestamp}: ${wrap(info.scriptName)}${wrap(info.customerId)} ${
        info.message
      }`
  )
);
export const defaultTransports: winston.transport[] = [];
defaultTransports.push(
  new winston.transports.Console({
    format: format.combine(...formats),
  })
);

export function createConsoleLogger() {
  const logger = winston.createLogger({
    silent: LOG_LEVEL === 'off',
    level: LOG_LEVEL, // NOTE: we use same log level for all transports
    format: format.combine(
      format.errors({ stack: true }),
      format.timestamp({ format: "YYYY-MM-DD HH:mm:ss:SSS" }),
      //format.json()
    ),
    // format: format.combine(
    //   format.timestamp({ format: "YYYY-MM-DD HH:mm:ss:SSS" })
    // ),
    transports: defaultTransports,
  });
  return logger;
}

export function createCloudLogger() {
  const cloudLogger = winston.createLogger({
    level: LOG_LEVEL,
    format: format.combine(
      format.errors({ stack: true }),
      format((info) => {
        info.trace = process.env.TRACE_ID;
        info[LOGGING_TRACE_KEY] = process.env.TRACE_ID;
        return info;
      })(),
      //format.json()
    ),
    defaultMeta: getDefaultMetadataForTracing(),
    transports: [
      new LoggingWinston({
        projectId: process.env.GCP_PROJECT,
        labels: {
          component: <string>process.env.LOG_COMPONENT,
        },
        logName: "gaarf",
        resource: {
          labels: {
            function_name: <string>process.env.K_SERVICE,
          },
          type: "cloud_function",
        },
        useMessageField: false,
        redirectToStdout: true,
      }),
    ],
  });
  return cloudLogger;
}

export function createLogger() {
  if (process.env.K_SERVICE) {
    // we're in Google Cloud (Run/Functions)
    return createCloudLogger();
  } else {
    return createConsoleLogger();
  }
}

