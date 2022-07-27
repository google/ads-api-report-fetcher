import express from 'express';
import { getFileContent } from 'google-ads-api-report-fetcher';
import path from 'path';

export async function getScript(req: express.Request):
    Promise<{queryText: string, scriptName: string}> {
  let scriptPath = req.query.script_path;
  let body = req.body || {};
  let queryText: string;
  let scriptName: string;
  if (body.script) {
    queryText = body.query;
    scriptName = body.name;
    console.log(`Executing inline query from request`);
  } else {
    queryText = await getFileContent(<string>scriptPath);
    scriptName = path.basename(<string>scriptPath).split('.sql')[0];
    console.log(`Executing query from '${scriptPath}'`);
  }
  if (!queryText)
    throw new Error(
        `Script was not specified in either script_path query argument or body.query`);
  if (!scriptName) throw new Error(`Could not determine script name`);
  return {queryText, scriptName};
}