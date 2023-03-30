"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.createLogger = void 0;
const logging_1 = require("@google-cloud/logging");
function createLogger(req, projectId, component) {
    const logging = new logging_1.Logging({ projectId: projectId });
    const log = logging.log('gaarf');
    const log_method = cloud_log.bind(null, log, req, projectId);
    const logger = {
        info: async (message, aux) => {
            return log_method('INFO', message, component, aux);
        },
        warn: async (message, aux) => {
            return log_method('WARN', message, component, aux);
        },
        error: async (message, aux) => {
            return log_method('ERROR', message, component, aux);
        },
    };
    process.env.LOG_COMPONENT = component;
    process.env.GCP_PROJECT = projectId;
    // const loggingWinston = new LoggingWinston({
    //   projectId: projectId,
    //   resource: {
    //     labels: {
    //       function_name: component,
    //     },
    //     type: 'cloud_function',
    //   },
    //   labels: {
    //     component: component,
    //   },
    //   redirectToStdout: true,
    // });
    // TODO: integrate with the logger from Gaarf
    //const transports = [loggingWinston];
    //let logger2 = gaarf_createLogger(transports);
    //gaarf_logger.transports.splice(0, gaarf_logger.transports.length);
    //gaarf_logger.transports.push(loggingWinston);
    return logger;
}
exports.createLogger = createLogger;
async function cloud_log(log, req, project, severity, message, component, aux) {
    const metadata = {
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
    else {
        const trace = 2;
        metadata.trace = `projects/${project}/traces/${trace}`;
        process.env.TRACE_ID = metadata.trace;
        // logging.googleapis.com/trace
        // LOGGING_TRACE_KEY
    }
    const entry = log.entry(metadata, aux ? Object.assign(aux || {}, { text: message }) : message);
    await log.write(entry);
}
//# sourceMappingURL=logger.js.map