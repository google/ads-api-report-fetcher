import express from 'express';
import {GoogleAuth} from 'google-auth-library';
import {
  getFileContent,
  GoogleAdsApiConfig,
  loadAdsConfigYaml,
  getLogger,
  getMemoryUsage,
  ILogger,
} from 'google-ads-api-report-fetcher';
import path from 'node:path';
import fs from 'node:fs';

export async function getScript(
  req: express.Request,
  logger: ILogger
): Promise<{queryText: string; scriptName: string}> {
  const scriptPath = req.query.script_path;
  const body = req.body || {};
  let queryText: string | undefined;
  let scriptName: string | undefined;
  if (body.script) {
    queryText = body.script.query;
    scriptName = body.script.name;
    logger.info('Executing inline query from request');
  } else if (scriptPath) {
    queryText = await getFileContent(<string>scriptPath);
    scriptName = path.basename(<string>scriptPath).split('.sql')[0];
    logger.info(`Executing query from '${scriptPath}'`);
  }
  if (!queryText)
    throw new Error(
      'Script was not specified in either script_path query argument or body.query'
    );
  if (!scriptName) throw new Error('Could not determine script name');
  return {queryText, scriptName};
}

export async function getAdsConfig(
  req: express.Request
): Promise<GoogleAdsApiConfig> {
  let adsConfig: GoogleAdsApiConfig | undefined;
  const adsConfigFile =
    <string>req.query.ads_config_path || process.env.ADS_CONFIG;
  if (adsConfigFile) {
    adsConfig = await loadAdsConfigYaml(adsConfigFile);
  } else if (req.body && req.body.ads_config) {
    // get from request body
    adsConfig = <GoogleAdsApiConfig>{
      developer_token: <string>req.body.ads_config.developer_token,
      login_customer_id: <string>req.body.ads_config.login_customer_id,
      client_id: <string>req.body.ads_config.client_id,
      client_secret: <string>req.body.ads_config.client_secret,
      refresh_token: <string>req.body.ads_config.refresh_token,
    };
  } else if (
    process.env.REFRESH_TOKEN &&
    process.env.DEVELOPER_TOKEN &&
    process.env.CLIENT_ID &&
    process.env.CLIENT_SECRET
  ) {
    // get from environment variables
    adsConfig = <GoogleAdsApiConfig>{
      developer_token: <string>process.env.DEVELOPER_TOKEN,
      login_customer_id: <string>process.env.LOGIN_CUSTOMER_ID,
      client_id: <string>process.env.CLIENT_ID,
      client_secret: <string>process.env.CLIENT_SECRET,
      refresh_token: <string>process.env.REFRESH_TOKEN,
    };
  } else if (fs.existsSync('google-ads.yaml')) {
    // get from a local file
    adsConfig = await loadAdsConfigYaml('google-ads.yaml');
  }
  if (
    !adsConfig ||
    !adsConfig.developer_token ||
    !adsConfig.refresh_token ||
    !adsConfig.client_id ||
    !adsConfig.client_secret
  ) {
    throw new Error('Ads API configuration is not complete.');
  }

  return adsConfig;
}

export async function getProject() {
  const auth = new GoogleAuth({
    scopes: 'https://www.googleapis.com/auth/cloud-platform',
  });
  const projectId = await auth.getProjectId();
  return projectId;
}

export function splitIntoChunks(array: Array<any>, max: number) {
  const result = [];
  for (let i = 0; i < array.length; i += max) {
    result.push(array.slice(i, i + max));
  }
  return result;
}

/**
 * Start a periodic logging of memory usage in backgroung.
 * @param logger logger to write to
 * @param intervalMs interval in milliseconds
 * @returns a callback to call for stopping logging
 */
export function startPeriodicMemoryLogging(logger: ILogger, intervalMs = 5000) {
  const intervalId = setInterval(() => {
    logger.info(getMemoryUsage('Periodic'));
  }, intervalMs);
  return () => clearInterval(intervalId); // Return function to stop logging
}
