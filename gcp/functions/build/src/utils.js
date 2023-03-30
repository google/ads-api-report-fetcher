"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getProject = exports.getAdsConfig = exports.getScript = void 0;
const google_auth_library_1 = require("google-auth-library");
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const node_path_1 = __importDefault(require("node:path"));
const node_fs_1 = __importDefault(require("node:fs"));
async function getScript(req, logger) {
    const scriptPath = req.query.script_path;
    const body = req.body || {};
    let queryText;
    let scriptName;
    if (body.script) {
        queryText = body.script.query;
        scriptName = body.script.name;
        await logger.info('Executing inline query from request');
    }
    else if (scriptPath) {
        queryText = await (0, google_ads_api_report_fetcher_1.getFileContent)(scriptPath);
        scriptName = node_path_1.default.basename(scriptPath).split('.sql')[0];
        await logger.info(`Executing query from '${scriptPath}'`);
    }
    if (!queryText)
        throw new Error('Script was not specified in either script_path query argument or body.query');
    if (!scriptName)
        throw new Error('Could not determine script name');
    return { queryText, scriptName };
}
exports.getScript = getScript;
async function getAdsConfig(req) {
    let adsConfig;
    const adsConfigFile = req.query.ads_config_path || process.env.ADS_CONFIG;
    if (adsConfigFile) {
        adsConfig = await (0, google_ads_api_report_fetcher_1.loadAdsConfigYaml)(adsConfigFile, req.query.customer_id);
    }
    else if (req.body && req.body.ads_config) {
        // get from request body
        adsConfig = {
            developer_token: req.body.ads_config.developer_token,
            login_customer_id: req.body.ads_config.login_customer_id,
            client_id: req.body.ads_config.client_id,
            client_secret: req.body.ads_config.client_secret,
            refresh_token: req.body.ads_config.refresh_token,
        };
    }
    else if (process.env.REFRESH_TOKEN &&
        process.env.DEVELOPER_TOKEN &&
        process.env.CLIENT_ID &&
        process.env.CLIENT_SECRET) {
        // get from environment variables
        adsConfig = {
            developer_token: process.env.DEVELOPER_TOKEN,
            login_customer_id: process.env.LOGIN_CUSTOMER_ID,
            client_id: process.env.CLIENT_ID,
            client_secret: process.env.CLIENT_SECRET,
            refresh_token: process.env.REFRESH_TOKEN,
        };
    }
    else if (node_fs_1.default.existsSync('google-ads.yaml')) {
        // get from a local file
        adsConfig = await (0, google_ads_api_report_fetcher_1.loadAdsConfigYaml)('google-ads.yaml', req.query.customer_id);
    }
    if (!adsConfig ||
        !adsConfig.developer_token ||
        !adsConfig.refresh_token ||
        !adsConfig.client_id ||
        !adsConfig.client_secret) {
        throw new Error('Ads API configuration is not complete.');
    }
    return adsConfig;
}
exports.getAdsConfig = getAdsConfig;
async function getProject() {
    const auth = new google_auth_library_1.GoogleAuth({
        scopes: 'https://www.googleapis.com/auth/cloud-platform',
    });
    const projectId = await auth.getProjectId();
    return projectId;
}
exports.getProject = getProject;
//# sourceMappingURL=utils.js.map