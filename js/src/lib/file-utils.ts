import fs from 'fs';

import { getFileFromGCS } from "./google-cloud";

export async function getFileContent(scriptPath: string): Promise<string> {
  if (scriptPath.startsWith('gcs://')) {
    return getFileFromGCS(scriptPath);
  }
  let queryText = fs.readFileSync(scriptPath.trim(), 'utf-8');
  return queryText;
}
