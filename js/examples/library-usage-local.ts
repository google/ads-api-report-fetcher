import {
  GoogleAdsApiClient,
  AdsQueryExecutor,
  loadAdsConfigFromFile,
  CsvWriter,
  getCustomerIds,
} from '../dist/index.js';
import * as fs from 'fs';
import * as path from 'path';

async function main() {
  try {
    const adsConfig = await loadAdsConfigFromFile('google-ads.yaml');
    const client = new GoogleAdsApiClient(adsConfig);
    
    const seedCid = adsConfig.customer_id || '1234567890';
    console.log(`Getting customer IDs for seed CID: ${seedCid}...`);
    let customers = await getCustomerIds(client, seedCid);

    let writer = new CsvWriter({ outputPath: '.tmp' });
    let executor = new AdsQueryExecutor(client);
    let params = {};
    let scriptPaths = ['examples/sample_query.sql'];

    console.log('Starting execution...');
    for (let scriptPath of scriptPaths) {
      let queryText = fs.readFileSync(scriptPath, 'utf-8');
      let scriptName = path.basename(scriptPath).split('.sql')[0];
      console.log(`Executing ${scriptName}...`);
      await executor.execute(scriptName, queryText, customers, params, writer);
    }
    console.log('Execution completed.');
  } catch (error) {
    console.error('Error occurred:', error);
  }
}

main();
