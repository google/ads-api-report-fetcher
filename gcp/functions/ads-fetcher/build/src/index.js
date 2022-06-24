"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.main = void 0;
const fs_1 = __importDefault(require("fs"));
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const js_yaml_1 = __importDefault(require("js-yaml"));
const path_1 = __importDefault(require("path"));
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;
const main = async (req, res) => {
    console.log(req.query);
    let body = req.body || {};
    // prepare Ads API parameters
    let adsConfig;
    let adsConfigFile = process.env.ADS_CONFIG || 'google-ads.yaml';
    if (fs_1.default.existsSync(adsConfigFile)) {
        adsConfig = js_yaml_1.default.load(fs_1.default.readFileSync(adsConfigFile, { encoding: 'utf-8' }));
    }
    else {
        adsConfig = {
            developer_token: process.env.developer_token,
            login_customer_id: process.env.login_customer_id,
            client_id: process.env.client_id,
            client_secret: process.env.client_secret,
            refresh_token: process.env.refresh_token
        };
    }
    console.log('Ads API config:');
    console.log(adsConfig);
    if (!adsConfig.developer_token || !adsConfig.refresh_token) {
        throw new Error(`Ads API configuration is not complete.`);
    }
    let scriptPath = req.query.script_path;
    if (!scriptPath)
        throw new Error(`Ads script path is not specified in script_path query argument`);
    let projectId = req.query.project_id || process.env.PROJECT_ID;
    if (!projectId)
        throw new Error(`Project id is not specified in either 'project_id' query argument or PROJECT_ID envvar`);
    let dataset = req.query.dataset || process.env.DATASET;
    if (!dataset)
        throw new Error(`Dataset is not specified in either 'dataset' query argument or DATASET envvar`);
    let customerId = req.query.customer_id;
    if (!customerId)
        throw new Error(`Customer id is not specified in 'customer_id' query argument`);
    let ads_client = new google_ads_api_report_fetcher_1.GoogleAdsApiClient(adsConfig, customerId);
    let executor = new google_ads_api_report_fetcher_1.AdsQueryExecutor(ads_client);
    let writer = new google_ads_api_report_fetcher_1.BigQueryWriter(projectId, dataset, { keepData: true });
    let singleCustomer = req.query.single_customer;
    let macros = req.body;
    let queryText = await (0, google_ads_api_report_fetcher_1.getFileContent)(scriptPath);
    let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
    if (singleCustomer) {
        let query = executor.parseQuery(queryText, macros);
        await writer.beginScript(scriptName, query);
        await executor.executeOne(query, customerId, writer);
        await writer.endScript();
    }
    else {
        console.log('Fetching customer ids');
        let customers = await ads_client.getCustomerIds();
        console.log(`Customers to process (${customers.length}):`);
        console.log(customers);
        await executor.execute(scriptName, queryText, customers, macros, writer);
    }
    let result = Object.entries(writer.rowsByCustomer).map(p => {
        return { [p[0]]: p[1].length };
    });
    res.send(result);
};
exports.main = main;
//# sourceMappingURL=index.js.map