/*
 Copyright 2025 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import {
  FieldType,
  FieldTypeKind,
  ProtoFieldMeta,
  ProtoTypeMeta,
  ProtoEnumMeta,
  // isEnumType, // Will be used when getFieldType's enum handling is complete
} from './types.js';
import axios from 'axios';
import fsPromises from 'fs/promises'; // Use fs/promises
import fs from 'fs'; // For readdirSync in static method
import path from 'path';
import {fileURLToPath} from 'url';
import {getLogger, ILogger} from './logger.js';
import {camelToSnakeCase} from './utils.js';
import {getFileFromGCS, saveFileToGCS} from './google-cloud.js';

export const AdsApiDefaultVersion = 'v23';

// Helper to get __dirname in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export interface IAdsApiSchema {
  getResource(name: string): Promise<ProtoTypeMeta & {name: string}>;
  getFieldType(type: ProtoTypeMeta, nameParts: string[]): Promise<FieldType>;
  getTypePrimitiveFields(
    type: ProtoTypeMeta,
  ): Promise<Array<ProtoFieldMeta & {name: string}>>;
}

// Helper functions
function snakeToPascalCase(str: string): string {
  return str
    .split('_')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

function snakeToCamelCase(str: string): string {
  return str.replace(/_([a-z])/g, (_match, letter) => letter.toUpperCase());
}

export class AdsApiSchemaRest implements IAdsApiSchema {
  private logger = getLogger();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private loadedSchemaPromise: Promise<any>;
  private schemaCache: Record<string, ProtoTypeMeta & {name: string}> = {};
  public readonly version: string;

  private static getLatestLocalSchemaVersion(logger: ILogger): string {
    const schemasDirs: string[] = [];
    if (
      process.env.GAARF_SCHEMA_DIR &&
      !process.env.GAARF_SCHEMA_DIR.startsWith('gs://')
    ) {
      schemasDirs.push(process.env.GAARF_SCHEMA_DIR);
    }
    schemasDirs.push(path.resolve(__dirname, 'schemas'));

    const allVersions: string[] = [];

    for (const schemasDir of schemasDirs) {
      try {
        const entries = fs.readdirSync(schemasDir, {withFileTypes: true});
        const versions = entries
          .filter(entry => entry.isDirectory() && /^v\d+$/.test(entry.name))
          .map(entry => entry.name);
        allVersions.push(...versions);
      } catch (error) {
        logger.warn(
          `Could not read local schema versions from ${schemasDir}:`,
          error,
        );
      }
    }

    if (allVersions.length > 0) {
      allVersions.sort((a, b) => parseInt(b.substring(1)) - parseInt(a.substring(1)));
      logger.debug(
        `Determined latest local schema version: ${allVersions[0]} from combined directories`,
      );
      return allVersions[0];
    }

    logger.warn(
      `Could not determine latest local schema version, defaulting to ${AdsApiDefaultVersion}`,
    );
    return AdsApiDefaultVersion; // Fallback
  }

  constructor(version?: string) {
    this.version =
      version || AdsApiSchemaRest.getLatestLocalSchemaVersion(this.logger);
    if (!this.version.startsWith('v')) {
      this.version = 'v' + this.version;
    }
    this.loadedSchemaPromise = this.loadSchema(this.version);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async getLoadedSchema(): Promise<any> {
    return this.loadedSchemaPromise;
  }

  async loadSchema(version: string) {
    const localSchemaDir = path.resolve(__dirname, 'schemas', version);
    const localSchemaPath = path.join(localSchemaDir, 'api-schema.json');
    let customSchemaPath = '';

    if (process.env.GAARF_SCHEMA_DIR) {
      if (process.env.GAARF_SCHEMA_DIR.startsWith('gs://')) {
        customSchemaPath = `${process.env.GAARF_SCHEMA_DIR}/${version}/api-schema.json`;
      } else {
        customSchemaPath = path.join(
          process.env.GAARF_SCHEMA_DIR,
          version,
          'api-schema.json',
        );
      }
    }

    // 1. Try local bundled schema
    try {
      const schemaData = await fsPromises.readFile(localSchemaPath, 'utf8');
      this.logger.debug(`Loaded schema from ${localSchemaPath}`);
      return JSON.parse(schemaData);
    } catch (_) {
      // ignore
    }

    // 2. Try GAARF_SCHEMA_DIR if configured
    if (customSchemaPath) {
      try {
        let schemaData;
        if (customSchemaPath.startsWith('gs://')) {
          schemaData = await getFileFromGCS(customSchemaPath);
        } else {
          schemaData = await fsPromises.readFile(customSchemaPath, 'utf8');
        }
        this.logger.debug(`Loaded schema from ${customSchemaPath}`);
        return JSON.parse(schemaData);
      } catch (_) {
        // ignore
      }
    }

    // 3. Download from API
    this.logger.info(
      `Schema not found locally at ${localSchemaPath}${customSchemaPath ? ` or ${customSchemaPath}` : ''}. Fetching schema for version ${version} from Google Ads API...`,
    );
    let schema;
    try {
      const response = await axios.get(
        `https://googleads.googleapis.com/$discovery/rest?version=${version}`,
        {headers: {Accept: 'application/json'}},
      );
      schema = response.data;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (apiErr: any) {
      throw new Error(`Failed to fetch schema: ${apiErr.message}`);
    }

    // 4. Save to GAARF_SCHEMA_DIR if specified, otherwise local bundled dir
    const savePath = customSchemaPath || localSchemaPath;
    this.logger.info(`Saving schema to ${savePath}`);
    try {
      if (savePath.startsWith('gs://')) {
        await saveFileToGCS(savePath, JSON.stringify(schema, null, 2));
      } else {
        await fsPromises.mkdir(path.dirname(savePath), {recursive: true});
        await fsPromises.writeFile(savePath, JSON.stringify(schema, null, 2));
      }
    } catch (e) {
      this.logger.warn(`Failed to save schema to ${savePath}: ${e}`);
    }

    return schema;
  }

  async getResource(name: string): Promise<ProtoTypeMeta & {name: string}> {
    if (this.schemaCache[name]) {
      return this.schemaCache[name];
    }

    const loadedSchema = await this.getLoadedSchema();
    if (!loadedSchema || !loadedSchema.schemas) {
      throw new Error('REST API schema has not been loaded or is invalid.');
    }

    const rowSchemaId = `GoogleAdsGoogleads${this.version.toUpperCase()}Services__GoogleAdsRow`;
    const rowSchema = loadedSchema.schemas[rowSchemaId];

    let restSchemaObject;
    let schemaId = name;

    const camelCaseName = snakeToCamelCase(name);
    if (
      rowSchema &&
      rowSchema.properties &&
      rowSchema.properties[camelCaseName]
    ) {
      const propDetails = rowSchema.properties[camelCaseName];
      if (propDetails.$ref) {
        schemaId = propDetails.$ref.replace(/^#\/schemas\//, '');
        restSchemaObject = loadedSchema.schemas[schemaId];
      }
    }

    if (!restSchemaObject) {
      const pascalCaseName = snakeToPascalCase(name);
      schemaId = `GoogleAdsGoogleads${this.version.toUpperCase()}Resources__${pascalCaseName}`;
      restSchemaObject =
        loadedSchema.schemas[schemaId] || loadedSchema.schemas[name];
    }

    if (!restSchemaObject) {
      this.logger.error(
        `[AdsApiSchemaRest.getResource] restSchemaObject is null for name: ${name}. Available keys in loadedSchema.schemas:`,
        Object.keys(loadedSchema.schemas),
      );
      throw new Error(
        `Schema for '${name}' (checked ${schemaId}) not found in REST API schema.`,
      );
    } else {
      this.logger.debug(
        `[AdsApiSchemaRest.getResource] Found restSchemaObject for ${schemaId}. Properties:`,
        Object.keys(restSchemaObject.properties || {}),
      );
    }

    const protoFields: Record<string, ProtoFieldMeta> = {};
    if (restSchemaObject.properties) {
      for (const propName in restSchemaObject.properties) {
        const snakePropName = camelToSnakeCase(propName);
        const propDetails = restSchemaObject.properties[propName];
        protoFields[snakePropName] =
          this.transformRestPropertyToProtoFieldMeta(propDetails);
      }
    }

    const transformedResource: ProtoTypeMeta & {name: string} = {
      name: (restSchemaObject.id || schemaId).split('__').pop() as string,
      fields: protoFields,
      nested: {},
    };

    this.schemaCache[name] = transformedResource;
    return transformedResource;
  }

  private transformRestPropertyToProtoFieldMeta(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    propDetails: any,
  ): ProtoFieldMeta {
    let fieldType = 'unknown';
    let rule: 'repeated' | undefined = undefined;

    if (propDetails.type === 'array') {
      rule = 'repeated';
      if (propDetails.items) {
        if (propDetails.items.$ref) {
          fieldType = propDetails.items.$ref.replace(/^#\/schemas\//, '');
        } else {
          fieldType = propDetails.items.type || 'unknown_array_item';
          if (propDetails.items.format) {
            fieldType = this.mapRestFormatToType(
              propDetails.items.type,
              propDetails.items.format,
            );
          }
        }
      }
    } else if (propDetails.$ref) {
      fieldType = propDetails.$ref.replace(/^#\/schemas\//, '');
    } else if (propDetails.type) {
      fieldType = this.mapRestFormatToType(
        propDetails.type,
        propDetails.format,
      );
    }

    return {
      type: fieldType,
      rule: rule,
      id: -1,
      options: {},
    };
  }

  private mapRestFormatToType(type: string, format?: string): string {
    if (format) {
      switch (format) {
        case 'int64':
          return 'int64';
        case 'int32':
          return 'int32';
        case 'double':
          return 'double';
        case 'float':
          return 'float';
        case 'byte':
          return 'bytes';
        case 'google-fieldmask':
          return 'google.protobuf.FieldMask';
      }
    }
    if (type === 'boolean') return 'bool';
    if (type === 'integer') return 'int64'; // Default to 64 bit unless formatted
    if (type === 'number') return 'double';
    return type;
  }

  async getFieldType(
    parentType: ProtoTypeMeta,
    nameParts: string[],
  ): Promise<FieldType> {
    if (!nameParts || nameParts.length === 0) {
      throw new Error('ArgumentException: nameParts is empty');
    }
    if (!parentType || !parentType.name) {
      throw new Error(
        'ArgumentException: parentType or parentType.name was not specified',
      );
    }

    const loadedSchema = await this.getLoadedSchema();
    if (!loadedSchema || !loadedSchema.schemas) {
      throw new Error('REST API schema has not been loaded or is invalid.');
    }

    let currentRestSchema = loadedSchema.schemas[parentType.name];
    if (!currentRestSchema) {
      const rowSchemaId = `GoogleAdsGoogleads${this.version.toUpperCase()}Services__GoogleAdsRow`;
      const rowSchema = loadedSchema.schemas[rowSchemaId];
      if (rowSchema && rowSchema.properties) {
        const camelName =
          parentType.name.charAt(0).toLowerCase() + parentType.name.slice(1);
        const propDetails = rowSchema.properties[camelName];
        if (propDetails && propDetails.$ref) {
          currentRestSchema =
            loadedSchema.schemas[propDetails.$ref.replace(/^#\/schemas\//, '')];
        }
      }
    }
    if (!currentRestSchema) {
      const schemaId = `GoogleAdsGoogleads${this.version.toUpperCase()}Resources__${parentType.name}`;
      currentRestSchema = loadedSchema.schemas[schemaId];
    }
    if (!currentRestSchema) {
      throw new Error(
        `Could not find schema for parent type ID '${parentType.name}'`,
      );
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let finalPropDetails: any;
    let isRepeatedContext = false;

    for (let i = 0; i < nameParts.length; i++) {
      const snakeCasePart = nameParts[i];
      const camelCasePart = snakeToCamelCase(snakeCasePart);

      if (
        !currentRestSchema.properties ||
        (!currentRestSchema.properties[camelCasePart] &&
          !currentRestSchema.properties[snakeCasePart])
      ) {
        // NOTE: theriotically we can search for the field in other schemas to
        // let the user know that the field might be depricated
        throw new Error(
          `Field part '${snakeCasePart}' (tried '${camelCasePart}') not found in resource ${parentType.name} (schema '${currentRestSchema.id}')`,
        );
      }
      finalPropDetails =
        currentRestSchema.properties[camelCasePart] ||
        currentRestSchema.properties[snakeCasePart];

      if (finalPropDetails.type === 'array') {
        isRepeatedContext = true;
        if (finalPropDetails.items && finalPropDetails.items.$ref) {
          currentRestSchema =
            loadedSchema.schemas[
              finalPropDetails.items.$ref.replace(/^#\/schemas\//, '')
            ];
          if (!currentRestSchema) {
            throw new Error(
              `Could not resolve $ref '${finalPropDetails.items.$ref}' for array item`,
            );
          }
        } else if (finalPropDetails.items && finalPropDetails.items.type) {
          if (i < nameParts.length - 1) {
            throw new Error(
              `Cannot access further properties on an array of non-$ref items: ${snakeCasePart}`,
            );
          }
        } else {
          throw new Error(`Invalid array item definition for ${snakeCasePart}`);
        }
      } else if (finalPropDetails.$ref) {
        currentRestSchema =
          loadedSchema.schemas[
            finalPropDetails.$ref.replace(/^#\/schemas\//, '')
          ];
        if (!currentRestSchema) {
          throw new Error(`Could not resolve $ref '${finalPropDetails.$ref}'`);
        }
        isRepeatedContext = false;
      } else {
        if (i < nameParts.length - 1) {
          if (
            finalPropDetails.type === 'object' &&
            finalPropDetails.properties
          ) {
            currentRestSchema = finalPropDetails;
          } else {
            throw new Error(
              `Cannot access further properties on a non-object/non-$ref type: ${snakeCasePart}`,
            );
          }
        }
        isRepeatedContext = false;
      }
    }

    if (!finalPropDetails) {
      throw new Error(
        `Could not resolve field path '${nameParts.join('.')}' in type '${
          parentType.name
        }'`,
      );
    }

    let fieldTypeKind: FieldTypeKind;
    let determinedTypeName: string;
    let typeValue: string | ProtoTypeMeta | ProtoEnumMeta;

    if (finalPropDetails.enum) {
      fieldTypeKind = FieldTypeKind.enum;
      const currentFieldName = snakeToPascalCase(
        nameParts[nameParts.length - 1],
      );
      const owningTypeName = (currentRestSchema.id || parentType.name)
        .split('__')
        .pop();
      determinedTypeName = `${owningTypeName}${currentFieldName}`;

      const enumValues: Record<string, number> = {};
      finalPropDetails.enum.forEach((val: string, idx: number) => {
        enumValues[val] = idx;
      });
      typeValue = {name: determinedTypeName, values: enumValues, fields: {}};
    } else if (finalPropDetails.$ref) {
      fieldTypeKind = FieldTypeKind.struct;
      const fullSchemaId = finalPropDetails.$ref.replace(/^#\/schemas\//, '');
      const schemaIdToLookup = fullSchemaId;
      const referencedSchemaObject = loadedSchema.schemas[schemaIdToLookup];

      if (!referencedSchemaObject) {
        throw new Error(
          `Could not find schema for $ref ID: ${schemaIdToLookup}`,
        );
      }
      const protoFields: Record<string, ProtoFieldMeta> = {};
      if (referencedSchemaObject.properties) {
        for (const propName in referencedSchemaObject.properties) {
          protoFields[propName] = this.transformRestPropertyToProtoFieldMeta(
            referencedSchemaObject.properties[propName],
          );
        }
      }
      typeValue = {
        name: schemaIdToLookup,
        fields: protoFields,
        nested: {},
      };
      determinedTypeName =
        schemaIdToLookup.split('__').pop() || schemaIdToLookup;
    } else if (
      finalPropDetails.type === 'object' &&
      finalPropDetails.properties
    ) {
      fieldTypeKind = FieldTypeKind.struct;
      determinedTypeName =
        finalPropDetails.id || `${nameParts.join('_')}Struct`;
      const inlineStructFields: Record<string, ProtoFieldMeta> = {};
      for (const propName in finalPropDetails.properties) {
        inlineStructFields[propName] =
          this.transformRestPropertyToProtoFieldMeta(
            finalPropDetails.properties[propName],
          );
      }
      typeValue = {name: determinedTypeName, fields: inlineStructFields};
    } else if (finalPropDetails.type === 'array' && finalPropDetails.items) {
      if (finalPropDetails.items.type && !finalPropDetails.items.$ref) {
        fieldTypeKind = FieldTypeKind.primitive;
        determinedTypeName = this.mapRestFormatToType(
          finalPropDetails.items.type,
          finalPropDetails.items.format,
        );
        typeValue = determinedTypeName;
      } else if (finalPropDetails.items.$ref) {
        fieldTypeKind = FieldTypeKind.struct;
        const fullSchemaId = finalPropDetails.items.$ref.replace(
          /^#\/schemas\//,
          '',
        );
        const schemaIdToLookup = fullSchemaId;
        const referencedSchemaObject = loadedSchema.schemas[schemaIdToLookup];
        if (!referencedSchemaObject) {
          throw new Error(
            `Could not find schema for array item $ref ID: ${schemaIdToLookup}`,
          );
        }
        const protoFields: Record<string, ProtoFieldMeta> = {};
        if (referencedSchemaObject.properties) {
          for (const propName in referencedSchemaObject.properties) {
            protoFields[propName] = this.transformRestPropertyToProtoFieldMeta(
              referencedSchemaObject.properties[propName],
            );
          }
        }
        typeValue = {
          name: schemaIdToLookup,
          fields: protoFields,
          nested: {},
        };
        determinedTypeName =
          schemaIdToLookup.split('__').pop() || schemaIdToLookup;
      } else {
        fieldTypeKind = FieldTypeKind.primitive;
        determinedTypeName = 'unknown_array_item_type';
        typeValue = determinedTypeName;
      }
    } else {
      fieldTypeKind = FieldTypeKind.primitive;
      determinedTypeName = this.mapRestFormatToType(
        finalPropDetails.type,
        finalPropDetails.format,
      );
      typeValue = determinedTypeName;
    }

    return {
      repeated: isRepeatedContext,
      kind: fieldTypeKind,
      typeName: determinedTypeName,
      type: typeValue,
    };
  }

  async getTypePrimitiveFields(
    type: ProtoTypeMeta,
  ): Promise<Array<ProtoFieldMeta & {name: string}>> {
    const loadedSchema = await this.getLoadedSchema();
    if (!loadedSchema || !loadedSchema.schemas || !type.name) {
      throw new Error('Schema not loaded or type name missing.');
    }
    let restSchemaObject = loadedSchema.schemas[type.name];
    if (!restSchemaObject) {
      const rowSchemaId = `GoogleAdsGoogleads${this.version.toUpperCase()}Services__GoogleAdsRow`;
      const rowSchema = loadedSchema.schemas[rowSchemaId];
      if (rowSchema && rowSchema.properties) {
        const camelName =
          type.name.charAt(0).toLowerCase() + type.name.slice(1);
        const propDetails = rowSchema.properties[camelName];
        if (propDetails && propDetails.$ref) {
          restSchemaObject =
            loadedSchema.schemas[propDetails.$ref.replace(/^#\/schemas\//, '')];
        }
      }
    }
    if (!restSchemaObject) {
      const schemaId = `GoogleAdsGoogleads${this.version.toUpperCase()}Resources__${type.name}`;
      restSchemaObject = loadedSchema.schemas[schemaId];
    }
    if (!restSchemaObject || !restSchemaObject.properties) {
      this.logger.warn(
        `Schema object for type name '${type.name}' not found or has no properties.`,
      );
      return [];
    }

    const primitiveFields: Array<ProtoFieldMeta & {name: string}> = [];
    for (const propName in restSchemaObject.properties) {
      const propDetails = restSchemaObject.properties[propName];
      const isPrimitive =
        propDetails.type === 'string' ||
        propDetails.type === 'integer' ||
        propDetails.type === 'number' ||
        propDetails.type === 'boolean';

      if (
        propDetails.type !== 'array' &&
        !propDetails.$ref &&
        (isPrimitive || propDetails.enum)
      ) {
        const name = camelToSnakeCase(propName);
        if (type.fields && type.fields[propName]) {
          primitiveFields.push(
            Object.assign({}, type.fields[propName], {name}),
          );
        } else {
          primitiveFields.push(
            Object.assign(
              this.transformRestPropertyToProtoFieldMeta(propDetails),
              {name},
            ),
          );
        }
      }
    }
    return primitiveFields;
  }
}
