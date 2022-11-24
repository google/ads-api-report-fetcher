"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAdsConfig = exports.getScript = void 0;
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const node_path_1 = __importDefault(require("node:path"));
const node_fs_1 = __importDefault(require("node:fs"));
async function getScript(req) {
    const scriptPath = req.query.script_path;
    const body = req.body || {};
    let queryText;
    let scriptName;
    if (body.script) {
        queryText = body.script.query;
        scriptName = body.script.name;
        console.log('Executing inline query from request');
    }
    else if (scriptPath) {
        queryText = await (0, google_ads_api_report_fetcher_1.getFileContent)(scriptPath);
        scriptName = node_path_1.default.basename(scriptPath).split('.sql')[0];
        console.log(`Executing query from '${scriptPath}'`);
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
    else {
        adsConfig = {
            developer_token: process.env.DEVELOPER_TOKEN,
            login_customer_id: process.env.LOGIN_CUSTOMER_ID,
            client_id: process.env.CLIENT_ID,
            client_secret: process.env.CLIENT_SECRET,
            refresh_token: process.env.REFRESH_TOKEN,
        };
    }
    if (!adsConfig && node_fs_1.default.existsSync('google-ads.yaml')) {
        adsConfig = await (0, google_ads_api_report_fetcher_1.loadAdsConfigYaml)('google-ads.yaml', req.query.customer_id);
    }
    return adsConfig;
}
exports.getAdsConfig = getAdsConfig;
//# sourceMappingURL=utils.js.map