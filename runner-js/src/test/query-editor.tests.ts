/**
 * Copyright 2022 Google LLC
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

import {AdsQueryEditor} from '../lib/ads-query-editor';
import {FieldTypeKind} from '../lib/types';

suite('AdsQueryEditor', () => {
  test('parse aliases', function() {
    let editor = new AdsQueryEditor();
    let query_text = `
      SELECT campaign.id, customer.id AS customer_id FROM campaign
    `;
    let query = editor.parseQuery(query_text, {});
    assert.deepEqual(query.columnNames, ['campaign_id', 'customer_id'])
    assert.deepEqual(
        query.columnTypes.map(t => t.typeName), ['int64', 'int64']);
  });
  test('handle hanging comma in select list', function() {
    let editor = new AdsQueryEditor();
    let query_text = `
      SELECT
        customer.id AS customer_id,
      FROM campaign
    `;
    let query = editor.parseQuery(query_text, {});
    assert.deepEqual(query.queryText, 'SELECT customer.id FROM campaign');
  });
  test('nested field', function() {
    let editor = new AdsQueryEditor();
    let queryText = `
      SELECT
          ad_group_ad.ad.responsive_display_ad.marketing_images:asset AS asset_id
      FROM ad_group_ad
    `;
    let query = editor.parseQuery(queryText, {});
    assert(query.columnTypes[0].repeated);
    assert.equal(query.columnTypes[0].typeName, 'string');
    assert.deepEqual(
        query.customizers[0], {type: 'NestedField', selector: 'asset'});
    assert.deepEqual(
        query.fields,
        ['ad_group_ad.ad.responsive_display_ad.marketing_images']);
    assert.deepEqual(query.columnNames, ['asset_id']);
  });
  test('nested field2', function() {
    let editor = new AdsQueryEditor();
    let queryText = `SELECT
      campaign.frequency_caps AS frequency_caps_raw,
      campaign.frequency_caps:key.level AS frequency_caps_level,
      FROM campaign
    `;
    let query = editor.parseQuery(queryText, {});
    assert(query.columnTypes[0].repeated);
    assert.equal(query.columnTypes[0].kind, FieldTypeKind.struct);
    assert.equal(query.columnTypes[0].typeName, 'FrequencyCapEntry');
    assert(query.columnTypes[1].repeated);
    assert.equal(query.columnTypes[1].kind, FieldTypeKind.enum);
    assert.equal(query.columnTypes[1].typeName, 'FrequencyCapLevel');
  });
});
