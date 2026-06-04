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
import axios from 'axios';
import fsPromises from 'fs/promises';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getLogger } from './logger.js';
import { getFileFromGCS, saveFileToGCS } from './google-cloud.js';
import { AdsApiDefaultVersion } from './ads-api-schema-base.js';
// Helper to get __dirname in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
export class RestSchemaLoader {
    constructor() {
        this.logger = getLogger();
    }
    getLatestVersion() {
        const schemasDirs = [];
        if (process.env.GAARF_SCHEMA_DIR &&
            !process.env.GAARF_SCHEMA_DIR.startsWith('gs://')) {
            schemasDirs.push(process.env.GAARF_SCHEMA_DIR);
        }
        schemasDirs.push(path.resolve(__dirname, 'schemas'));
        const allVersions = [];
        for (const schemasDir of schemasDirs) {
            try {
                const entries = fs.readdirSync(schemasDir, { withFileTypes: true });
                const versions = entries
                    .filter(entry => entry.isDirectory() && /^v\d+$/.test(entry.name))
                    .map(entry => entry.name);
                allVersions.push(...versions);
            }
            catch (error) {
                this.logger.warn(`Could not read local schema versions from ${schemasDir}:`, error);
            }
        }
        if (allVersions.length > 0) {
            allVersions.sort((a, b) => Number(b.substring(1)) - Number(a.substring(1)));
            this.logger.debug(`Determined latest local schema version: ${allVersions[0]} from combined directories`);
            return allVersions[0];
        }
        this.logger.warn(`Could not determine latest local schema version, defaulting to ${AdsApiDefaultVersion}`);
        return AdsApiDefaultVersion;
    }
    async loadSchema(version) {
        const localSchemaDir = path.resolve(__dirname, 'schemas', version);
        const localSchemaPath = path.join(localSchemaDir, 'api-schema.json');
        let customSchemaPath = '';
        if (process.env.GAARF_SCHEMA_DIR) {
            if (process.env.GAARF_SCHEMA_DIR.startsWith('gs://')) {
                customSchemaPath = `${process.env.GAARF_SCHEMA_DIR}/${version}/api-schema.json`;
            }
            else {
                customSchemaPath = path.join(process.env.GAARF_SCHEMA_DIR, version, 'api-schema.json');
            }
        }
        // 1. Try local bundled schema
        try {
            const schemaData = await fsPromises.readFile(localSchemaPath, 'utf8');
            this.logger.debug(`Loaded schema from ${localSchemaPath}`);
            return JSON.parse(schemaData);
        }
        catch (_) {
            // ignore
        }
        // 2. Try GAARF_SCHEMA_DIR if configured
        if (customSchemaPath) {
            try {
                let schemaData;
                if (customSchemaPath.startsWith('gs://')) {
                    schemaData = await getFileFromGCS(customSchemaPath);
                }
                else {
                    schemaData = await fsPromises.readFile(customSchemaPath, 'utf8');
                }
                this.logger.debug(`Loaded schema from ${customSchemaPath}`);
                return JSON.parse(schemaData);
            }
            catch (_) {
                // ignore
            }
        }
        // 3. Download from API
        this.logger.info(`Schema not found locally at ${localSchemaPath}${customSchemaPath ? ` or ${customSchemaPath}` : ''}. Fetching schema for version ${version} from Google Ads API...`);
        let schema;
        try {
            const response = await axios.get(`https://googleads.googleapis.com/$discovery/rest?version=${version}`, { headers: { Accept: 'application/json' } });
            schema = response.data;
        }
        catch (apiErr) {
            throw new Error(`Failed to fetch schema: ${apiErr.message}`);
        }
        // 4. Save to GAARF_SCHEMA_DIR if specified, otherwise local bundled dir
        const savePath = customSchemaPath || localSchemaPath;
        this.logger.info(`Saving schema to ${savePath}`);
        try {
            if (savePath.startsWith('gs://')) {
                await saveFileToGCS(savePath, JSON.stringify(schema, null, 2));
            }
            else {
                await fsPromises.mkdir(path.dirname(savePath), { recursive: true });
                await fsPromises.writeFile(savePath, JSON.stringify(schema, null, 2));
            }
        }
        catch (e) {
            this.logger.warn(`Failed to save schema to ${savePath}: ${e}`);
        }
        return schema;
    }
}
//# sourceMappingURL=ads-api-schema-loader-rest.js.map