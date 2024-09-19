"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.startPeriodicMemoryLogging = exports.splitIntoChunks = exports.getProject = exports.getAdsConfig = exports.getScript = void 0;
const google_auth_library_1 = require("google-auth-library");
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const node_path_1 = __importDefault(require("node:path"));
const node_fs_1 = __importDefault(require("node:fs"));
/**
 * Get script from request body or from a file specified in query parameters.
 * @param req request object
 * @param logger logger to write to
 * @returns a promise that resolves to an object with `queryText` and `scriptName`
 * properties
 */
async function getScript(req, logger) {
    const scriptPath = req.query.script_path;
    const body = req.body || {};
    let queryText;
    let scriptName;
    if (body.script) {
        queryText = body.script.query;
        scriptName = body.script.name;
        logger.info('Executing inline query from request');
    }
    else if (scriptPath) {
        queryText = await (0, google_ads_api_report_fetcher_1.getFileContent)(scriptPath);
        scriptName = node_path_1.default.basename(scriptPath).split('.sql')[0];
        logger.info(`Executing query from '${scriptPath}'`);
    }
    if (!queryText)
        throw new Error('Script was not specified in either script_path query argument or body.query');
    if (!scriptName)
        throw new Error('Could not determine script name');
    return { queryText, scriptName };
}
exports.getScript = getScript;
/**
 * Get Ads API configuration from request body or from a file specified in query
 * parameters.
 * @param req request object
 * @returns a promise that resolves to an object with Ads API configuration
 */
async function getAdsConfig(req) {
    let adsConfig;
    const adsConfigFile = req.query.ads_config_path || process.env.ADS_CONFIG;
    if (adsConfigFile) {
        adsConfig = await (0, google_ads_api_report_fetcher_1.loadAdsConfigYaml)(adsConfigFile);
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
        adsConfig = await (0, google_ads_api_report_fetcher_1.loadAdsConfigYaml)('google-ads.yaml');
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
/**
 * Get project id from environment variables.
 * @returns a promise that resolves to a project id
 */
async function getProject() {
    const auth = new google_auth_library_1.GoogleAuth({
        scopes: 'https://www.googleapis.com/auth/cloud-platform',
    });
    const projectId = await auth.getProjectId();
    return projectId;
}
exports.getProject = getProject;
/**
 * Split an array into chunks of a given size.
 * @param array array to split
 * @param max maximum size of a chunk
 * @returns an array of arrays
 */
function splitIntoChunks(array, max) {
    const result = [];
    for (let i = 0; i < array.length; i += max) {
        result.push(array.slice(i, i + max));
    }
    return result;
}
exports.splitIntoChunks = splitIntoChunks;
/**
 * Start a periodic logging of memory usage in backgroung.
 * @param logger logger to write to
 * @param intervalMs interval in milliseconds
 * @returns a callback to call for stopping logging
 */
function startPeriodicMemoryLogging(logger, intervalMs = 5000) {
    const intervalId = setInterval(() => {
        logger.info((0, google_ads_api_report_fetcher_1.getMemoryUsage)('Periodic'));
    }, intervalMs);
    return () => clearInterval(intervalId); // Return function to stop logging
}
exports.startPeriodicMemoryLogging = startPeriodicMemoryLogging;
//# sourceMappingURL=utils.js.map