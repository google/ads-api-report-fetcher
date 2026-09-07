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

export const createRequire = () => () => ({ argv: {} });
export class Storage {}
export class File {}
export class GoogleAuth {
  getAccessToken(): Promise<string> {
    throw new Error(
      'GoogleAuth is not supported in browser environment. Please provide access_token in GoogleAdsApiConfig.',
    );
  }
  getProjectId(): Promise<string> {
    return Promise.resolve('');
  }
}
export const existsSync = () => false;
export const readdirSync = () => [];
export const statSync = () => ({ size: 0 });
export const promises = {
  readFile: () => Promise.reject(new Error('fs is not available in browser')),
  writeFile: () => Promise.reject(new Error('fs is not available in browser')),
  mkdir: () => Promise.reject(new Error('fs is not available in browser')),
};

export const fileURLToPath = () => '';
export const dirname = () => '';
export const resolve = () => '';
export const join = () => '';
export const getFileFromGCS = () => Promise.resolve('');
export const saveFileToGCS = () => Promise.resolve();

export default {};
