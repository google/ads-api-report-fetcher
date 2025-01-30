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
import winston from 'winston';
import { LoggingWinston, getDefaultMetadataForTracing, LOGGING_TRACE_KEY, } from '@google-cloud/logging-winston';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;
const { format } = winston;
/** Default log level */
// NOTE: as we use argv directly (before parsing) we have to manually check all aliases for the option log-level
export const LOG_LEVEL = argv.logLevel ||
    argv.loglevel ||
    argv.ll ||
    argv.log_level ||
    process.env.LOG_LEVEL ||
    (process.env.NODE_ENV === 'dev' ? 'verbose' : 'info');
const colors = {
    error: 'red',
    warn: 'yellow',
    info: 'white',
    verbose: 'gray',
    debug: 'grey',
};
function wrap(str) {
    return str ? ' [' + str + ']' : '';
}
const formats = [];
if (process.stdout.isTTY) {
    formats.push(format.colorize({ all: true }));
    winston.addColors(colors);
}
formats.push(format.printf(info => `${info.timestamp}: ${wrap(info.scriptName)}${wrap(info.customerId)} ${info.message}`));
export const defaultTransports = [];
defaultTransports.push(new winston.transports.Console({
    format: format.combine(...formats),
    handleRejections: LOG_LEVEL === 'debug',
}));
export function createConsoleLogger() {
    const logger = winston.createLogger({
        silent: LOG_LEVEL === 'off',
        level: LOG_LEVEL, // NOTE: we use same log level for all transports
        format: format.combine(format.errors({ stack: true }), format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss:SSS' })),
        transports: defaultTransports,
        exitOnError: false,
    });
    return logger;
}
export function createCloudLogger() {
    const cloudLogger = winston.createLogger({
        level: LOG_LEVEL,
        format: format.combine(format.errors({ stack: true }), format(info => {
            info.trace = process.env.TRACE_ID;
            info[LOGGING_TRACE_KEY] = process.env.TRACE_ID;
            return info;
        })()),
        defaultMeta: getDefaultMetadataForTracing(),
        transports: [
            new LoggingWinston({
                projectId: process.env.GCP_PROJECT,
                labels: {
                    component: process.env.LOG_COMPONENT,
                },
                logName: 'gaarf',
                resource: {
                    labels: {
                        function_name: process.env.K_SERVICE,
                    },
                    type: 'cloud_function',
                },
                useMessageField: false,
                // setting redirectToStdout:true actually disables using Logging API,
                // and simply dumps entries to stdout where the logger agent
                // parses them and redirect to Logging.
                // It's the only way to overcome sporadic errors
                // during calling Logging API:
                // "GoogleError: Total timeout of API google.logging.v2.LoggingServiceV2
                // exceeded 60000 milliseconds before any response was received"
                // See https://github.com/googleapis/nodejs-logging/issues/1185
                // And even recommended in
                // https://cloud.google.com/nodejs/docs/reference/logging-winston/latest#alternative-way-to-ingest-logs-in-google-cloud-managed-environments
                redirectToStdout: true,
                handleRejections: LOG_LEVEL === 'debug',
            }),
        ],
        exitOnError: false,
    });
    return cloudLogger;
}
export function createLogger() {
    let logger;
    if (process.env.K_SERVICE) {
        // we're in Google Cloud (Run/Functions)
        logger = createCloudLogger();
    }
    else {
        logger = createConsoleLogger();
    }
    logger.on('error', e => {
        console.error(`Error on logging: ${e}`);
    });
    return logger;
}
//# sourceMappingURL=logger-factory.js.map