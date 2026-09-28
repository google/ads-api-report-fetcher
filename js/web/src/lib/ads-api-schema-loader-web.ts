/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import axios from 'axios';
import {
  ISchemaLoader,
  AdsApiDefaultVersion,
} from '../../../src/lib/ads-api-schema-base.js';

export interface WebSchemaLoaderOptions {
  /**
   * Custom discovery REST API endpoint URL.
   * Default: 'https://googleads.googleapis.com/$discovery/rest'
   */
  discoveryUrl?: string;
  /**
   * Whether to cache downloaded schemas in localStorage.
   * Default: true
   */
  useCache?: boolean;
}

export class WebSchemaLoader implements ISchemaLoader {
  private options: WebSchemaLoaderOptions;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private schemaCache: Map<string, any> = new Map();

  constructor(options?: WebSchemaLoaderOptions) {
    this.options = {
      discoveryUrl: 'https://googleads.googleapis.com/$discovery/rest',
      useCache: true,
      ...options,
    };
  }

  getLatestVersion(): string {
    return AdsApiDefaultVersion;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async loadBundledSchema(): Promise<any> {
    const mod = await import('./bundled-schema.js');
    return mod.default || mod;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async loadSchema(version: string): Promise<any> {
    const normVersion = version.startsWith('v') ? version : 'v' + version;

    // 1. In-memory cache check
    if (this.schemaCache.has(normVersion)) {
      return this.schemaCache.get(normVersion);
    }

    // 2. Bundled schema match -> Dynamic import
    if (normVersion === AdsApiDefaultVersion) {
      try {
        const bundled = await this.loadBundledSchema();
        this.schemaCache.set(normVersion, bundled);
        return bundled;
      } catch (e) {
        console.warn(
          'Could not dynamically load bundled schema, attempting discovery fetch:',
          e,
        );
      }
    }

    // 3. Browser localStorage cache check
    const cacheKey = `gaarf_schema_${normVersion}`;
    if (
      this.options.useCache &&
      typeof window !== 'undefined' &&
      window.localStorage
    ) {
      try {
        const cached = window.localStorage.getItem(cacheKey);
        if (cached) {
          const parsed = JSON.parse(cached);
          this.schemaCache.set(normVersion, parsed);
          return parsed;
        }
      } catch (_) {
        // Ignore localStorage read errors
      }
    }

    // 4. Fetch from Google Ads Discovery API
    const url = `${this.options.discoveryUrl}?version=${normVersion}`;
    try {
      const response = await axios.get(url, {
        headers: {Accept: 'application/json'},
      });
      const schema = response.data;
      if (!schema || typeof schema !== 'object' || !schema.schemas) {
        throw new Error(
          `Invalid discovery schema response received for ${normVersion}`,
        );
      }

      this.schemaCache.set(normVersion, schema);

      if (
        this.options.useCache &&
        typeof window !== 'undefined' &&
        window.localStorage
      ) {
        try {
          window.localStorage.setItem(cacheKey, JSON.stringify(schema));
        } catch (_) {
          // Ignore localStorage write errors
        }
      }

      return schema;
    } catch (e: any) {
      const detail =
        e.response?.data?.error?.message || e.message || String(e);
      throw new Error(
        `Failed to load Google Ads API schema for ${normVersion}: ${detail}`,
      );
    }
  }
}

export class BundledSchemaLoader implements ISchemaLoader {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private cachedSchema?: any;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async loadSchema(version: string): Promise<any> {
    if (!this.cachedSchema) {
      const mod = await import('./bundled-schema.js');
      this.cachedSchema = mod.default || mod;
    }
    const bundledVersion = this.getLatestVersion();
    if (version !== bundledVersion && 'v' + version !== bundledVersion) {
      console.warn(
        `Requested schema version ${version} but only ${bundledVersion} is bundled. Returning ${bundledVersion} schema.`,
      );
    }
    return this.cachedSchema;
  }

  getLatestVersion(): string {
    return AdsApiDefaultVersion;
  }
}

export {WebSchemaLoader as RestSchemaLoader};
