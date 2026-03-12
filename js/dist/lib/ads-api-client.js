/**
 * Copyright 2025 Google LLC
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
import { GoogleAuth } from 'google-auth-library';
import { executeWithRetry } from './utils.js';
import { getLogger } from './logger.js';
import axios from 'axios';
import { AdsQueryEditor } from './ads-query-editor.js';
import { AdsRowParser } from './ads-row-parser.js';
import { AdsApiSchemaRest, AdsApiDefaultVersion, } from './ads-api-schema.js';
export class GoogleAdsError extends Error {
    constructor(message) {
        super(message || 'Unknown error on calling Google Ads API occurred');
        this.logged = false;
        this.retryable = false;
    }
}
/**
 * Base class for Google Ads API clients.
 */
export class GoogleAdsApiClientBase {
    constructor(adsConfig, apiVersion) {
        if (!adsConfig) {
            throw new Error('GoogleAdsApiConfig instance was not passed');
        }
        this.adsConfig = adsConfig;
        this.logger = getLogger();
        if (apiVersion && !apiVersion.startsWith('v')) {
            apiVersion = 'v' + apiVersion;
        }
        this.apiVersion = apiVersion || AdsApiDefaultVersion;
        this.schema = new AdsApiSchemaRest(this.apiVersion);
    }
    getQueryEditor() {
        return new AdsQueryEditor(this.schema);
    }
    getRowParser() {
        return new AdsRowParser(this.logger);
    }
}
/**
 * Google Ads API client using REST API.
 */
export class GoogleAdsRestApiClient extends GoogleAdsApiClientBase {
    constructor(adsConfig, apiVersion) {
        super(adsConfig, apiVersion);
        this.currentToken = null;
        this.tokenExpiration = 0;
        this.refreshInterval = 300000; // 5 minutes
        this.authClient = null;
        this.baseUrl = `https://googleads.googleapis.com/${this.apiVersion}/`;
        if (this.adsConfig.json_key_file_path || !this.adsConfig.refresh_token) {
            this.authClient = new GoogleAuth({
                keyFile: this.adsConfig.json_key_file_path,
                scopes: 'https://www.googleapis.com/auth/adwords',
            });
        }
    }
    async refreshAccessToken(clientId, clientSecret, refreshToken) {
        var _a, _b;
        const tokenUrl = 'https://www.googleapis.com/oauth2/v3/token';
        const data = {
            client_id: clientId,
            client_secret: clientSecret,
            refresh_token: refreshToken,
            grant_type: 'refresh_token',
        };
        try {
            const response = await axios.post(tokenUrl, data);
            return {
                access_token: response.data.access_token,
                expires_in: response.data.expires_in || 3600,
            };
        }
        catch (error) {
            if (axios.isAxiosError(error)) {
                throw new Error(`Failed to refresh token: ${(_a = error.response) === null || _a === void 0 ? void 0 : _a.status}, ${JSON.stringify((_b = error.response) === null || _b === void 0 ? void 0 : _b.data)}`);
            }
            throw error;
        }
    }
    async getValidToken() {
        if (this.authClient) {
            // working under a service account
            const accessToken = await this.authClient.getAccessToken();
            return accessToken;
        }
        if (this.currentToken === null ||
            Date.now() >= this.tokenExpiration - this.refreshInterval) {
            // working under a user account (with refreshToken)
            // Refresh if within 5 minutes of expiration
            const { access_token, expires_in } = await this.refreshAccessToken(this.adsConfig.client_id, this.adsConfig.client_secret, this.adsConfig.refresh_token);
            this.currentToken = access_token;
            this.tokenExpiration = Date.now() + expires_in * 1000;
        }
        return this.currentToken;
    }
    async executeQuery(query, customerId) {
        this.logger.debug(`Executing GAQL query: ${query}`);
        const url = `${this.baseUrl}customers/${customerId}/googleAds:search`;
        const headers = await this.createHeaders();
        const payload = {
            query,
        };
        let results;
        do {
            const data = await executeWithRetry(async () => {
                try {
                    return await this.sendApiRequest(url, payload, headers);
                }
                catch (e) {
                    throw this.handleGoogleAdsError(e, customerId, query) || e;
                }
            }, (error, attempt) => {
                const retry = attempt <= 3 && error.retryable;
                this.logger.verbose(retry
                    ? `Retrying on transient error, attempt ${attempt}, error: ${error}`
                    : `Breaking on ${error.retryable ? 'retriable' : 'non-retriable'} error, attempt ${attempt}, error: ${error}`, { customerId, query });
                return retry;
            }, {
                baseDelayMs: 100,
                delayStrategy: 'linear',
            });
            if (data === null || data === void 0 ? void 0 : data.results) {
                if (!results) {
                    results = data.results;
                }
                else {
                    results = results.concat(data.results);
                }
            }
            if (data === null || data === void 0 ? void 0 : data.nextPageToken) {
                payload.pageToken = data.nextPageToken;
                continue;
            }
            break;
            // eslint-disable-next-line no-constant-condition
        } while (true);
        return results || [];
    }
    async createHeaders() {
        const headers = {
            Authorization: `Bearer ${await this.getValidToken()}`,
            'developer-token': this.adsConfig.developer_token,
            'Content-Type': 'application/json',
        };
        if (this.authClient) {
            headers['x-goog-user-project'] = await this.authClient.getProjectId();
        }
        if (this.adsConfig.login_customer_id) {
            headers['login-customer-id'] = this.adsConfig.login_customer_id;
        }
        return headers;
    }
    async *executeQueryStream(query, customerId) {
        this.logger.debug(`Executing GAQL query: ${query}`);
        const url = `${this.baseUrl}customers/${customerId}/googleAds:search`;
        const headers = await this.createHeaders();
        const payload = {
            query,
        };
        do {
            // The current implementation is using batched 'search' method,
            // simply iterating over results. Ideally we should use 'searchStream' method
            // with axios' responseType: 'stream' and parse results w/o buffering.
            // Additionally there's a difference how executeQueryStream and executeQuery
            // are used. The former is called by AdsQueryExecuter wrapped in executeWithRetry,
            // while the latter is expected to implement retry on its own.
            try {
                const data = await this.sendApiRequest(url, payload, headers);
                if (data === null || data === void 0 ? void 0 : data.results) {
                    for (const row of data.results) {
                        yield row;
                    }
                }
                if (data === null || data === void 0 ? void 0 : data.nextPageToken) {
                    payload.pageToken = data.nextPageToken;
                    continue;
                }
                break;
            }
            catch (e) {
                throw this.handleGoogleAdsError(e, customerId, query) || e;
            }
            // eslint-disable-next-line no-constant-condition
        } while (true);
    }
    async sendApiRequest(url, data, headers) {
        try {
            const response = await axios.post(url, data, {
                headers,
            });
            return response.data;
        }
        catch (error) {
            if (error.response && error.response.data) {
                let errData = error.response.data;
                if (errData.length)
                    errData = errData[0];
                if (errData === null || errData === void 0 ? void 0 : errData.error)
                    throw errData.error;
            }
            throw error;
        }
    }
    handleGoogleAdsError(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    error, customerId, query) {
        var _a, _b, _c;
        try {
            console.error(error);
            this.logger.error(`An error occurred on executing query (cid: ${customerId}): ${query}\nRaw error: ` +
                JSON.stringify(error, null, 2), { customerId, query });
        }
        catch (e) {
            // a very unfortunate situation
            console.error(e);
            this.logger.error(`An error occurred on executing query and on logging it afterwards: ${query}\n.Raw error: ${e}, logging error:${e}`);
        }
        const failure = error.details && error.details.length ? error.details[0] : null;
        if (!failure) {
            this.logger.debug('Could not parse API error into GoogleAdsFailure');
            error.logged = true;
            error.retryable = true;
            return error;
        }
        let message = error.message || 'Unknown Google Ads API error';
        if (error.status) {
            message = error.status + ': ' + message;
        }
        if (failure.errors && failure.errors.length) {
            message += ': ' + failure.errors[0].message;
        }
        const ex = new GoogleAdsError(message);
        const transientStatusCodes = [408, 429, 500, 502, 503, 504];
        if (error.code && transientStatusCodes.includes(error.code)) {
            ex.retryable = true;
        }
        if (failure.errors.length) {
            if (((_a = failure.errors[0].errorCode) === null || _a === void 0 ? void 0 : _a.internalError) ||
                ((_b = failure.errors[0].errorCode) === null || _b === void 0 ? void 0 : _b.quotaError)) {
                ex.retryable = true;
            }
        }
        else {
            // it's an unknown error (no `errors` collection), it happens sometimes
            // we'll treat such errors as retryable
            ex.retryable = true;
        }
        ex.account = customerId;
        ex.query = query;
        ex.logged = true;
        this.logger.debug(`API error parsed into GoogleAdsFailure: ${ex.message}, error_code: ${error.errors ? (_c = error.errors[0]) === null || _c === void 0 ? void 0 : _c.errorCode : ''})`, { customerId, query });
        return ex;
    }
}
//# sourceMappingURL=ads-api-client.js.map