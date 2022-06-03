"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getFileContent = void 0;
const fs_1 = __importDefault(require("fs"));
const google_cloud_1 = require("./google-cloud");
async function getFileContent(scriptPath) {
    if (scriptPath.startsWith('gcs://')) {
        return (0, google_cloud_1.getFileFromGCS)(scriptPath);
    }
    let queryText = fs_1.default.readFileSync(scriptPath.trim(), 'utf-8');
    return queryText;
}
exports.getFileContent = getFileContent;
//# sourceMappingURL=file-utils.js.map