import assert from 'assert';
import { AdsQueryEditor } from '../src/ads-query-editor';
import { FieldTypeKind } from '../src/types';

suite('AdsQueryEditor', () => {
  test('parse aliases', function() {
    let editor = new AdsQueryEditor();
    let query_text = `
      SELECT campaign.id, customer.id AS customer_id FROM campaign
    `;
    let query = editor.parseQuery(query_text, {});
    assert.deepEqual(query.columnNames, ['campaign.id', 'customer_id'])
    assert.deepEqual(query.columnTypes.map(t => t.typeName), ['int64', 'int64']);
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
    assert.deepEqual(query.customizers[0], { type: 'NestedField', value: 'asset' });
    assert.deepEqual(query.fields, ['ad_group_ad.ad.responsive_display_ad.marketing_images']);
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
