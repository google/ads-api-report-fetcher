import assert from 'assert';
import {AdsApiSchemaRest} from '../lib/ads-api-schema.js';

suite('AdsApiSchemaRest', () => {
  let schema: AdsApiSchemaRest;

  setup(() => {
    schema = new AdsApiSchemaRest();
  });

  test('Parsing schema for Campaign resource', async () => {
    // getResource implicitly uses transformRestPropertyToProtoFieldMeta
    // to map all fields on the resource, mapping them to snake_case.
    // We will verify a subset of well-known properties covering all types.
    const campaign = await schema.getResource('campaign');

    assert.ok(campaign);
    assert.strictEqual(campaign.name, 'Campaign');
    assert.ok(campaign.fields);

    // 1. Scalar Property (int64)
    // "id": { "format": "int64", "type": "string", "readOnly": true }
    const idField = campaign.fields['id'];
    assert.deepStrictEqual(
      idField,
      {
        type: 'int64',
        rule: undefined,
        id: -1,
        options: {},
      },
      'Scalar field "id" (int64) mismatch',
    );

    // 2. Scalar Property (string)
    // "name": { "type": "string" }
    const nameField = campaign.fields['name'];
    assert.deepStrictEqual(
      nameField,
      {
        type: 'string',
        rule: undefined,
        id: -1,
        options: {},
      },
      'Scalar field "name" (string) mismatch',
    );

    // 3. Enum Property inline (string with enum values)
    // "status": { "type": "string", "enum": [...] }
    const statusField = campaign.fields['status'];
    assert.deepStrictEqual(
      statusField,
      {
        // Notice it correctly maps REST string-based enums to 'string'
        type: 'string',
        rule: undefined,
        id: -1,
        options: {},
      },
      'Enum field "status" mismatch',
    );

    // 4. Repeated Array Property (strings)
    // "labels": { "type": "array", "items": { "type": "string" } }
    const labelsField = campaign.fields['labels'];
    assert.deepStrictEqual(
      labelsField,
      {
        type: 'string',
        rule: 'repeated',
        id: -1,
        options: {},
      },
      'Array field "labels" mismatch',
    );

    // 5. Message / Struct Property ($ref)
    // "networkSettings": { "$ref": "GoogleAdsGoogleadsV...__NetworkSettings" }
    const networkSettingsField = campaign.fields['network_settings'];
    assert.ok(
      networkSettingsField.type.includes('NetworkSettings'),
      `Struct field "network_settings" type mismatch. Got: ${networkSettingsField.type}`,
    );
    assert.strictEqual(networkSettingsField.rule, undefined);

    // 6. Repeated Array Property (struct/$ref)
    // "urlCustomParameters": { "type": "array", "items": { "$ref": "GoogleAdsGoogleadsV...__CustomParameter" } }
    const urlCustomParamsField = campaign.fields['url_custom_parameters'];
    assert.ok(
      urlCustomParamsField.type.includes('CustomParameter'),
      `Struct array field "url_custom_parameters" type mismatch. Got ${urlCustomParamsField.type}`,
    );
    assert.strictEqual(urlCustomParamsField.rule, 'repeated');
  });
});
