"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.GoogleAdsApiClient = void 0;
const fs_1 = __importDefault(require("fs"));
const google_ads_api_1 = require("google-ads-api");
const js_yaml_1 = __importDefault(require("js-yaml"));
const lodash_1 = __importDefault(require("lodash"));
class GoogleAdsApiClient {
    constructor(config, customerId) {
        let ads_cfg;
        if (lodash_1.default.isString(config)) {
            ads_cfg = this.loadConfig(config, customerId);
        }
        else {
            ads_cfg = config;
        }
        // let ads_cfg = this.loadConfig(config, customerId);
        if (!ads_cfg.customer_id) {
            throw new Error(`No customer id was specified`);
        }
        this.ads_cfg = ads_cfg;
        this.client = new google_ads_api_1.GoogleAdsApi({
            client_id: ads_cfg.client_id,
            client_secret: ads_cfg.client_secret,
            developer_token: ads_cfg.developer_token
        });
        this.customers = {};
        this.customers[customerId] = this.client.Customer({
            customer_id: ads_cfg.customer_id,
            login_customer_id: ads_cfg.login_customer_id,
            refresh_token: ads_cfg.refresh_token
        });
        // also put the customer as the default one
        this.customers[''] = this.customers[customerId];
    }
    async executeQuery(query, customerId) {
        let customer;
        if (!customerId) {
            customer = this.customers[''];
        }
        else {
            customer = this.customers[customerId];
            if (!customer) {
                customer = this.client.Customer({
                    customer_id: customerId,
                    login_customer_id: this.ads_cfg.login_customer_id,
                    refresh_token: this.ads_cfg.refresh_token
                });
                this.customers[customerId] = customer;
            }
        }
        try {
            return await customer.query(query);
        }
        catch (e) {
            let error = e;
            if (error.errors)
                console.log(`An error occured on executing query: ` +
                    JSON.stringify(error.errors[0]));
            throw e;
        }
    }
    async getCustomerIds() {
        var _a;
        // customer_client.descriptive_name,
        const query_customer_ids = `SELECT
          customer_client.id,
          customer_client.manager
        FROM customer_client`;
        let rows = await this.executeQuery(query_customer_ids);
        let ids = [];
        for (let row of rows) {
            if (row.customer_client && !((_a = row.customer_client) === null || _a === void 0 ? void 0 : _a.manager)) {
                ids.push(row.customer_client.id);
            }
        }
        return ids;
    }
    loadConfig(config_file_path, customerId) {
        try {
            if (!fs_1.default.existsSync(config_file_path))
                throw new Error(`Config file ${config_file_path} does not exist`);
            const doc = js_yaml_1.default.load(fs_1.default.readFileSync(config_file_path, 'utf8'));
            console.log(doc);
            return {
                developer_token: doc['developer_token'],
                client_id: doc['client_id'],
                client_secret: doc['client_secret'],
                refresh_token: doc['refresh_token'],
                login_customer_id: doc['login_customer_id'],
                customer_id: customerId || doc['customer_id']
            };
        }
        catch (e) {
            console.log('Failed to load Ads API configuration from ' + config_file_path);
            throw e;
        }
    }
}
exports.GoogleAdsApiClient = GoogleAdsApiClient;
//# sourceMappingURL=api-client.js.map