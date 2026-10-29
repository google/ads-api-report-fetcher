/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from 'assert';

import {AdsQueryEditor} from '../lib/ads-query-editor.js';
import {AdsApiSchemaRest} from '../lib/ads-api-schema-base.js';
import {FieldTypeKind} from '../lib/types.js';
import {RestSchemaLoader} from '../lib/ads-api-schema-loader-rest.js';

suite('AdsQueryEditor', () => {
  const schema = new AdsApiSchemaRest(new RestSchemaLoader());
  const editor = new AdsQueryEditor(schema);

  test('parse aliases', async () => {
    const query_text = `
      SELECT
        campaign.id,
        customer.id AS customer_id,
        campaign.target_cpa.target_cpa_micros,
        campaign.target_cpa.target_cpa_micros as campaign_cpa
      FROM campaign
    `;
    const query = await editor.parseQuery(query_text, {});
    assert.deepEqual(query.columnNames, [
      'id',
      'customer_id',
      'target_cpa_target_cpa_micros',
      'campaign_cpa',
    ]);
    assert.deepEqual(
      query.columnTypes.map(t => t.typeName),
      ['int64', 'int64', 'int64', 'int64'],
    );
  });

  test('handle hanging comma in select list', async () => {
    const query_text = `
      SELECT
        customer.id AS customer_id,
      FROM campaign
    `;
    const query = await editor.parseQuery(query_text, {});
    assert.deepEqual(query.queryText, 'SELECT customer.id FROM campaign');
  });

  test('nested field', async () => {
    const queryText = `
      SELECT
          ad_group_ad.ad.responsive_display_ad.marketing_images:asset AS asset_id
      FROM ad_group_ad
    `;
    const query = await editor.parseQuery(queryText, {});
    assert(query.columnTypes[0].repeated);
    assert.equal(query.columnTypes[0].typeName, 'string');
    assert.deepEqual(query.columns[0].customizer, {
      type: 'NestedField',
      selector: 'asset',
    });
    assert.deepEqual(query.columnNames, ['asset_id']);
  });

  test('nested field with 2 levels', async () => {
    const queryText = `SELECT
      campaign.frequency_caps AS frequency_caps_raw,
      campaign.frequency_caps:key.level AS frequency_caps_level,
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert(query.columnTypes[0].repeated);
    assert.equal(query.columnTypes[0].kind, FieldTypeKind.struct);
    assert.equal(query.columnTypes[0].typeName, 'FrequencyCapEntry');
    assert(query.columnTypes[1].repeated);
    assert.equal(query.columnTypes[1].kind, FieldTypeKind.enum);
    assert.equal(query.columnTypes[1].typeName, 'FrequencyCapKeyLevel');
  });

  test('virtual columns: operation with column and constant', async () => {
    const queryText = `SELECT
      campaign.target_cpa.target_cpa_micros / 1000000 AS target_cpa
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT campaign.target_cpa.target_cpa_micros FROM campaign',
    );
  });

  test('virtual columns: arithmetic operation with columns', async () => {
    const queryText = `SELECT
      metrics.clicks / metrics.impressions
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT metrics.clicks, metrics.impressions FROM campaign',
    );
  });

  test('virtual columns: method call on column value', async () => {
    const queryText = `SELECT
      (metrics.clicks / metrics.impressions).toFixed(2) as ctr,
      campaign.name.split('_').pop() as prefix
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT metrics.clicks, metrics.impressions, campaign.name FROM campaign',
    );
  });

  test('virtual columns: compatibility with resource indexes and nested fields', async () => {
    const queryText = `SELECT
      campaign.frequency_caps AS frequency_caps_raw,
      campaign.frequency_caps:key.level AS frequency_caps_level,
      'campaign: ' + campaign.name + '(' + campaign.id + ')' as title,
      '~' + campaign.name + '~' as name
      FROM campaign
    `;
    // symbols '~' and ':' in constants should not be confused with customizers
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT campaign.frequency_caps, campaign.name, campaign.id FROM campaign',
    );
  });

  test('remove comments', async () => {
    const query_text = `/* Copyleft (x) 2030
https://www.apache.org/licenses/LICENSE-2.0
*/
      SELECT #comment
        --campagin
        campaign.id -- campaign id
# comment
      FROM campaign /*comment*/
      /*WHERE campaign
      */
    `;
    const query = await editor.parseQuery(query_text, {});
    assert.deepEqual(query.queryText, 'SELECT campaign.id FROM campaign');
  });

  test('virtual columns: custom function math expr type inference (bool)', async () => {
    const queryText = `
      SELECT
      \`some(campaign.asset_automation_settings, f(s) = equalText(s.asset_automation_type, 'FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION') and equalText(s.asset_automation_status,'OPTED_OUT'))\` AS url_expansion_opt_out
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(query.columnTypes[0].typeName, 'bool');
  });

  test('virtual columns: custom function type inference (bool from array size)', async () => {
    const queryText = `
      SELECT
      \`size(filter(campaign.asset_automation_settings, f(s) = equalText(s.asset_automation_type,'FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION') and equalText(s.asset_automation_status,'OPTED_IN')))[1] > 0\` AS url_expansion_is_opted_out2
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(query.columnTypes[0].typeName, 'bool');
  });

  test('macros and identifiers starting with keyword prefixes (from_, as_, select_, limit_, where_)', async () => {
    const queryText = `
      SELECT
        as_number(campaign.name) AS name_num,
        from_micros(metrics.cost_micros) AS cost
      FROM campaign
      WHERE segments.date >= '{from_date}'
        AND segments.date <= '{limit_1}'
        AND campaign.name != '{where_filter}'
      FUNCTIONS
      function as_number(val) { return Number(val); }
      function from_micros(val) { return val / 1000000; }
    `;
    const query = await editor.parseQuery(queryText, {
      from_date: '2025-01-01',
      limit_1: '2025-01-31',
      where_filter: 'test',
    });
    assert.equal(
      query.queryText.replace(/\s+/g, ' '),
      "SELECT campaign.name, metrics.cost_micros FROM campaign WHERE segments.date >= '2025-01-01' AND segments.date <= '2025-01-31' AND campaign.name != 'test'",
    );
  });

  test('multi-line comment inside WHERE and SELECT clauses', async () => {
    const queryText = `
      SELECT
        metrics.clicks / /* inline comment */ metrics.impressions AS ctr
      FROM campaign
      WHERE campaign.id > 0/*inline comment*/AND campaign.status = 'ENABLED'
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText.replace(/\s+/g, ' '),
      "SELECT metrics.clicks, metrics.impressions FROM campaign WHERE campaign.id > 0 AND campaign.status = 'ENABLED'",
    );
  });

  test('string literals in WHERE containing comment markers (//, #, --, /*)', async () => {
    const queryText = `
      SELECT campaign.id
      FROM campaign
      WHERE campaign.tracking_url_template = 'https://example.com/page#section'
        AND campaign.name != 'Promo -- #1 /* not a comment */'
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText.replace(/\s+/g, ' '),
      "SELECT campaign.id FROM campaign WHERE campaign.tracking_url_template = 'https://example.com/page#section' AND campaign.name != 'Promo -- #1 /* not a comment */'",
    );
  });

  test('string literals in WHERE containing keywords and semicolons', async () => {
    const queryText = `
      SELECT campaign.id
      FROM campaign
      WHERE campaign.name = 'Select from best; order by limit 10'
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      "SELECT campaign.id FROM campaign WHERE campaign.name = 'Select from best; order by limit 10'",
    );
  });

  test('string literals in SELECT virtual columns containing commas and keywords (AS, FROM)', async () => {
    const queryText = `
      SELECT
        campaign.name.split(',')[1] AS first_part,
        'from ' + campaign.name + ' as text' AS label
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.deepEqual(query.columnNames, ['first_part', 'label']);
    assert.equal(query.queryText, 'SELECT campaign.name FROM campaign');
  });

  test('multi-argument function calls in SELECT without backticks', async () => {
    const queryText = `
      SELECT
        round(metrics.cost_micros / 1000000, 2) AS cost_rounded
      FROM campaign
    `;
    const query = await editor.parseQuery(queryText, {});
    assert.deepEqual(query.columnNames, ['cost_rounded']);
    assert.equal(query.queryText, 'SELECT metrics.cost_micros FROM campaign');
  });

  suite('strictness (reject invalid queries)', () => {
    test('reject missing comma between fields in SELECT', async () => {
      const queryText = `
        SELECT
          campaign.id
          campaign.name
        FROM campaign
      `;
      await assert.rejects(() => editor.parseQuery(queryText, {}));
    });

    test('reject missing AS keyword before alias in SELECT', async () => {
      const queryText = `
        SELECT
          campaign.id campaign
        FROM campaign
      `;
      await assert.rejects(() => editor.parseQuery(queryText, {}));
    });

    test('reject reserved keyword as column alias', async () => {
      const queryText = `
        SELECT
          campaign.id AS where
        FROM campaign
      `;
      await assert.rejects(() => editor.parseQuery(queryText, {}));
    });

    test('reject empty WHERE, ORDER BY, PARAMETERS, or FUNCTIONS clauses', async () => {
      await assert.rejects(() =>
        editor.parseQuery('SELECT campaign.id FROM campaign WHERE', {}),
      );
      await assert.rejects(() =>
        editor.parseQuery(
          'SELECT campaign.id FROM campaign WHERE /* only comment */',
          {},
        ),
      );
      await assert.rejects(() =>
        editor.parseQuery('SELECT campaign.id FROM campaign ORDER BY', {}),
      );
      await assert.rejects(() =>
        editor.parseQuery('SELECT campaign.id FROM campaign PARAMETERS', {}),
      );
      await assert.rejects(() =>
        editor.parseQuery('SELECT campaign.id FROM campaign FUNCTIONS', {}),
      );
    });
  });
});


