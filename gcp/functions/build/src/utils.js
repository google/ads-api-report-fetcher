"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getScript = void 0;
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const path_1 = __importDefault(require("path"));
async function getScript(req) {
    let scriptPath = req.query.script_path;
    let body = req.body || {};
    let queryText;
    let scriptName;
    if (body.script) {
        queryText = body.query;
        scriptName = body.name;
        console.log(`Executing inline query from request`);
    }
    else {
        queryText = await (0, google_ads_api_report_fetcher_1.getFileContent)(scriptPath);
        scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        console.log(`Executing query from '${scriptPath}'`);
    }
    if (!queryText)
        throw new Error(`Script was not specified in either script_path query argument or body.query`);
    if (!scriptName)
        throw new Error(`Could not determine script name`);
    return { queryText, scriptName };
}
exports.getScript = getScript;
//# sourceMappingURL=utils.js.map