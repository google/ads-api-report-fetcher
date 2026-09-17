/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from 'assert';
import {
  GoogleAdsApiClient,
  AdsQueryEditor,
  AdsQueryExecutor,
  AdsRowParser,
  AdsApiSchemaRest,
  WebSchemaLoader,
  BundledSchemaLoader,
  FieldTypeKind,
  substituteMacros,
} from '../index.js';

suite('Gaarf Web Flavor', () => {
  const schemaLoader = new BundledSchemaLoader();
  const schema = new AdsApiSchemaRest(schemaLoader);

  test('WebSchemaLoader provides bundled schema and version', async () => {
    const loader = new WebSchemaLoader();
    const version = loader.getLatestVersion();
    assert(version.startsWith('v'), `Expected version starting with v, got ${version}`);
    const loadedSchema = await loader.loadSchema(version);
    assert(loadedSchema && loadedSchema.schemas, 'Expected schemas in loaded schema');
  });

  test('GoogleAdsApiClient can be instantiated with access_token', () => {
    const client = new GoogleAdsApiClient(
      {
        access_token: 'test-access-token',
        login_customer_id: '1234567890',
      },
      undefined,
      schemaLoader,
    );
    assert.equal(client.apiVersion, schemaLoader.getLatestVersion());
    assert(client.getQueryEditor() instanceof AdsQueryEditor);
    assert(client.getRowParser() instanceof AdsRowParser);
  });

  test('AdsQueryEditor parses GAQL query with aliases and virtual columns', async () => {
    const editor = new AdsQueryEditor(schema);
    const queryText = `
      SELECT
        campaign.id AS campaign_id,
        campaign.name AS campaign_name,
        metrics.clicks / metrics.impressions AS ctr,
        campaign.target_cpa.target_cpa_micros / 1000000 AS target_cpa
      FROM campaign
      WHERE campaign.status = 'ENABLED'
    `;

    const parsed = await editor.parseQuery(queryText);
    assert.deepEqual(parsed.columnNames, [
      'campaign_id',
      'campaign_name',
      'ctr',
      'target_cpa',
    ]);
    assert.equal(
      parsed.queryText,
      'SELECT campaign.id, campaign.name, metrics.clicks, metrics.impressions, campaign.target_cpa.target_cpa_micros FROM campaign WHERE campaign.status = \'ENABLED\'',
    );
  });

  test('AdsQueryEditor parses nested fields and resource index customizers', async () => {
    const editor = new AdsQueryEditor(schema);
    const queryText = `
      SELECT
        ad_group_ad.ad.responsive_display_ad.marketing_images:asset AS asset_id,
        customer_client_link.client_customer~0 AS account_id
      FROM ad_group_ad
    `;

    const parsed = await editor.parseQuery(queryText);
    assert.equal(parsed.columns[0].customizer?.type, 'NestedField');
    assert.equal(parsed.columns[1].customizer?.type, 'ResourceIndex');
  });

  test('AdsRowParser transforms raw API responses using customizers and math expressions', () => {
    const rowParser = new AdsRowParser({
      info: () => {},
      warn: () => {},
      error: () => {},
      debug: () => {},
      verbose: () => {},
    } as any);

    const queryElements = {
      queryText: 'SELECT metrics.clicks, metrics.impressions FROM campaign',
      columns: [
        {
          name: 'campaign_name',
          expression: 'campaign.name',
          type: {kind: FieldTypeKind.primitive, type: 'string', typeName: 'string'},
        },
        {
          name: 'ctr',
          expression: 'metrics.clicks / metrics.impressions',
          type: {kind: FieldTypeKind.primitive, type: 'double', typeName: 'double'},
          customizer: {
            type: 'VirtualColumn',
            evaluator: {
              evaluate: (scope: any) =>
                scope['metrics.clicks'] / scope['metrics.impressions'],
            },
          },
        },
      ],
      resource: {name: 'campaign', typeName: 'Campaign', typeMeta: {} as any, isConstant: false},
      functions: {},
    } as any;

    const rawRow = {
      campaign: {
        name: 'Summer Sale',
      },
      metrics: {
        clicks: 50,
        impressions: 1000,
      },
    };

    const parsedRowArray = rowParser.parseRow(rawRow, queryElements, false) as unknown[];
    assert.deepEqual(parsedRowArray, ['Summer Sale', 0.05]);

    const parsedRowObject = rowParser.parseRow(rawRow, queryElements, true) as Record<string, unknown>;
    assert.equal(parsedRowObject['campaign_name'], 'Summer Sale');
    assert.equal(parsedRowObject['ctr'], 0.05);
  });

  test('Macro substitution works in web environment', () => {
    const queryWithMacros = `
      SELECT campaign.id
      FROM campaign
      WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
    `;

    const res = substituteMacros(queryWithMacros, {
      start_date: '2026-01-01',
      end_date: '2026-01-31',
    });

    assert.equal(
      res.text.trim(),
      "SELECT campaign.id\n      FROM campaign\n      WHERE segments.date >= '2026-01-01' AND segments.date <= '2026-01-31'",
    );
    assert.equal(res.unknown_params.length, 0);
  });
});
