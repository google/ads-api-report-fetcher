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
    const log_method = cloud_log.bind(null, log, req, projectId, component);
    const logger = {
        info: async (message, aux) => {
            return log_method('INFO', message, aux);
        },
        warn: async (message, aux) => {
            return log_method('WARN', message, aux);
        },
        error: async (message, aux) => {
            return log_method('ERROR', message, aux);
        },
    };
    // NOTE: here we're setting some environment variables for winston logger in gaarf library
    process.env.LOG_COMPONENT = component;
    process.env.GCP_PROJECT = projectId;
    return logger;
}
exports.createLogger = createLogger;
async function cloud_log(log, req, project, component, severity, message, aux) {
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
    const entry = log.entry(metadata, aux ? Object.assign(aux || {}, { text: message }) : message);
    await log.write(entry);
}
//# sourceMappingURL=logger.js.map