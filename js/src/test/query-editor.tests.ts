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
import {ApiType, FieldTypeKind} from '../lib/types.js';

suite('AdsQueryEditor', () => {
  const editor = new AdsQueryEditor(ApiType.gRPC, 'v20');

  test('parse aliases', () => {
    const query_text = `
      SELECT
        campaign.id,
        customer.id AS customer_id,
        campaign.target_cpa.target_cpa_micros,
        campaign.target_cpa.target_cpa_micros as campaign_cpa
      FROM campaign
    `;
    const query = editor.parseQuery(query_text, {});
    assert.deepEqual(query.columnNames, [
      'id',
      'customer_id',
      'target_cpa_target_cpa_micros',
      'campaign_cpa',
    ]);
    assert.deepEqual(
      query.columnTypes.map(t => t.typeName),
      ['int64', 'int64', 'int64', 'int64']
    );
  });

  test('handle hanging comma in select list', () => {
    const query_text = `
      SELECT
        customer.id AS customer_id,
      FROM campaign
    `;
    const query = editor.parseQuery(query_text, {});
    assert.deepEqual(query.queryText, 'SELECT customer.id FROM campaign');
  });

  test('nested field', () => {
    const queryText = `
      SELECT
          ad_group_ad.ad.responsive_display_ad.marketing_images:asset AS asset_id
      FROM ad_group_ad
    `;
    const query = editor.parseQuery(queryText, {});
    assert(query.columnTypes[0].repeated);
    assert.equal(query.columnTypes[0].typeName, 'string');
    assert.deepEqual(query.columns[0].customizer, {
      type: 'NestedField',
      selector: 'asset',
    });
    assert.deepEqual(query.columnNames, ['asset_id']);
  });

  test('nested field with 2 levels', () => {
    const queryText = `SELECT
      campaign.frequency_caps AS frequency_caps_raw,
      campaign.frequency_caps:key.level AS frequency_caps_level,
      FROM campaign
    `;
    const query = editor.parseQuery(queryText, {});
    assert(query.columnTypes[0].repeated);
    assert.equal(query.columnTypes[0].kind, FieldTypeKind.struct);
    assert.equal(query.columnTypes[0].typeName, 'FrequencyCapEntry');
    assert(query.columnTypes[1].repeated);
    assert.equal(query.columnTypes[1].kind, FieldTypeKind.enum);
    assert.equal(query.columnTypes[1].typeName, 'FrequencyCapLevel');
  });

  test('virtual columns: operation with column and constant', () => {
    const queryText = `SELECT
      campaign.target_cpa.target_cpa_micros / 1000000 AS target_cpa
      FROM campaign
    `;
    const query = editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT campaign.target_cpa.target_cpa_micros FROM campaign'
    );
  });

  test('virtual columns: arithmetic operation with columns', () => {
    const queryText = `SELECT
      metrics.clicks / metrics.impressions
      FROM campaign
    `;
    const query = editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT metrics.clicks, metrics.impressions FROM campaign'
    );
  });

  test('virtual columns: method call on column value', () => {
    const queryText = `SELECT
      (metrics.clicks / metrics.impressions).toFixed(2) as ctr,
      campaign.name.split('_').pop() as prefix
      FROM campaign
    `;
    const query = editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT metrics.clicks, metrics.impressions, campaign.name FROM campaign'
    );
  });

  test('virtual columns: compatibility with resource indexes and nested fields', () => {
    const queryText = `SELECT
      campaign.frequency_caps AS frequency_caps_raw,
      campaign.frequency_caps:key.level AS frequency_caps_level,
      'campaign: ' + campaign.name + '(' + campaign.id + ')' as title,
      '~' + campaign.name + '~' as name
      FROM campaign
    `;
    // symbols '~' and ':' in constants should not be confused with customizers
    const query = editor.parseQuery(queryText, {});
    assert.equal(
      query.queryText,
      'SELECT campaign.frequency_caps, campaign.name, campaign.id FROM campaign'
    );
  });

  test('remove comments', () => {
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
    const query = editor.parseQuery(query_text, {});
    assert.deepEqual(query.queryText, 'SELECT campaign.id FROM campaign');
  });
});
