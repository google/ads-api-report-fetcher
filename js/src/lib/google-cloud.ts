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
import { Storage } from '@google-cloud/storage';

export async function getFileFromGCS(filePath: string): Promise<string> {
  let parsed = new URL(filePath);
  let bucket = parsed.hostname;
  let filename = parsed.pathname.substring(1);

  return new Promise((resolve, reject) => {
    const storage = new Storage();
    let fileContents = Buffer.from('');
    storage.bucket(bucket)
        .file(filename)
        .createReadStream()
        .on('error',
            (err) => {
              reject(
                  `Failed to download '${filePath}' file content from GCS: ` +
                  err);
            })
        .on('data',
            (chunk) => {
              fileContents = Buffer.concat([fileContents, chunk]);
            })
        .on('end', () => {
          let content = fileContents.toString('utf8');
          resolve(content);
        });
  });
}
