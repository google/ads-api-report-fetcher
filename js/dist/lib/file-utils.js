"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getFileContent = void 0;
/**
 * Copyright 2023 Google LLC
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
const fs_1 = __importDefault(require("fs"));
const google_cloud_1 = require("./google-cloud");
async function getFileContent(scriptPath) {
    if (scriptPath.startsWith('gs://')) {
        return (0, google_cloud_1.getFileFromGCS)(scriptPath);
    }
    let queryText = fs_1.default.readFileSync(scriptPath.trim(), 'utf-8');
    return queryText;
}
exports.getFileContent = getFileContent;
//# sourceMappingURL=file-utils.js.map