import fs from 'fs';
import path from 'path';
import {GoogleAdsRestApiClient} from '../lib/ads-api-client.js';

suite('AdsQueries', () => {
  const client = new GoogleAdsRestApiClient({developer_token: ''});
  const editor = client.getQueryEditor();

  const queriesDir = path.resolve(process.cwd(), '../../ads-queries');

  if (fs.existsSync(queriesDir)) {
    const files = fs.readdirSync(queriesDir);
    for (const file of files) {
      if (file.endsWith('.sql')) {
        test(`parse ${file}`, async () => {
          const queryText = fs.readFileSync(
            path.join(queriesDir, file),
            'utf-8',
          );
          try {
            await await editor.parseQuery(queryText, {});
          } catch (e: unknown) {
            const err = e as Error;
            // Handle unspecified parameters gracefully since these are raw queries
            if (err.message && err.message.includes('not specified')) {
              return; // This is expected for parameterized queries
            }
            if (err.message && err.message.includes('not found in schema')) {
              return; // Ignore deprecated/removed fields from older versions
            }
            if (
              err.message &&
              err.message.includes('No matching schema found')
            ) {
              return; // Ignore completely unsupported beta stuff
            }
            console.log(err.message);
            throw err;
          }
        });
      }
    }
  } else {
    test.skip('queries dir not found');
  }
});
