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
/**
 * Copyright 2026 Google LLC
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

import {ISchemaLoader} from '../../../src/lib/ads-api-schema-base.js';
import bundledSchema from './bundled-schema.js';

export class BundledSchemaLoader implements ISchemaLoader {
  async loadSchema(version: string): Promise<any> {
    const bundledVersion = this.getLatestVersion();
    if (version !== bundledVersion) {
      console.warn(
        `Requested schema version ${version} but only ${bundledVersion} is bundled. Returning ${bundledVersion} schema.`,
      );
    }
    return bundledSchema;
  }

  getLatestVersion(): string {
    return bundledSchema.version;
  }
}
