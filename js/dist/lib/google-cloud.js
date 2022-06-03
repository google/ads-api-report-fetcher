"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getFileFromGCS = void 0;
const storage_1 = require("@google-cloud/storage");
async function getFileFromGCS(filePath) {
    let parsed = new URL(filePath);
    let bucket = parsed.hostname;
    let filename = parsed.pathname.substring(1);
    return new Promise((resolve, reject) => {
        const storage = new storage_1.Storage();
        let fileContents = Buffer.from('');
        storage.bucket(bucket)
            .file(filename)
            .createReadStream()
            .on('error', (err) => {
            reject(`Failed to download '${filePath}' file content from GCS: ` +
                err);
        })
            .on('data', (chunk) => {
            fileContents = Buffer.concat([fileContents, chunk]);
        })
            .on('end', () => {
            let content = fileContents.toString('utf8');
            resolve(content);
        });
    });
}
exports.getFileFromGCS = getFileFromGCS;
//# sourceMappingURL=google-cloud.js.map