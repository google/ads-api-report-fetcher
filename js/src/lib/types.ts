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

import { EvalFunction } from "mathjs";

export interface CustomizerResourceIndex {
  type: CustomizerType.ResourceIndex;
  index: number;
}
export interface CustomizerSelector {
  type: CustomizerType.NestedField;
  selector: string;
}
export interface CustomizerFunction {
  type: CustomizerType.Function;
  function: string;
}
export interface CustomizerVirtualColumn {
  type: CustomizerType.VirtualColumn;
  evaluator: EvalFunction
}

export type Customizer =
  | CustomizerResourceIndex
  | CustomizerSelector
  | CustomizerFunction
  | CustomizerVirtualColumn;

export enum CustomizerType {
  ResourceIndex = 'ResourceIndex',
  NestedField = 'NestedField',
  Function = "Function",
  VirtualColumn = "VirtualColumn"
}

export enum FieldTypeKind {
  primitive,
  enum,
  struct
}
export interface ProtoFieldMeta {
  rule?: 'repeated';
  /**
   * field's type can be:
   *  - a primitive type (string, int64, float, bool)
   *  - type short name ("NetworkSettings"), then it should be inside `nested`
        field of the same type
   *  - a common type full name
        ("google.ads.googleads.v9.common.RealTimeBiddingSetting")
   *  - enum
        ("google.ads.googleads.v9.enums.CampaignExperimentTypeEnum.CampaignExperimentType")
   *  - resource name, actually it's a string (type="string"), but with
        additional options (`"(google.api.resource_reference).type":
        "googleads.googleapis.com/Feed"`)
   */
  type: string;
  // position in protobuf, unimportant
  id: number;
  // additional options
  options: Record<string, string>;
}
export interface ProtoTypeMeta {
  name?: string;  // extension
  options: any;
  /**
   * Type fields
   */
  fields: Record<string, ProtoFieldMeta>;
  /**
   * Nested types
   */
  nested: Record<string, ProtoTypeMeta>;
}
export interface ProtoEnumMeta {
  values: Record<string, number>;
  name?: string;  // extension
}
export function isEnumType(type: any): type is ProtoEnumMeta {
  return !!type.values;
}
export interface ResourceInfo {
  name: string;      // "campaign_criterion"
  typeName: string;  // "CampaignCriterion"
  //fullName: string;  // "google.ads.googleads.v9.resources.CampaignCriterion"
  typeMeta: ProtoTypeMeta;  // resource type description
  isConstant: boolean;
}
export interface FieldType {
  repeated?: boolean;
  kind: FieldTypeKind;
  typeName: string;
  type: string|ProtoTypeMeta|ProtoEnumMeta
}

export interface Column {
  name: string;
  expression: string;
  type: FieldType;
  customizer: Customizer | null | undefined;
}
export class QueryElements {
  queryText: string = "";
  columns: Column[];
  resource: ResourceInfo;
  functions: Record<string, Function>;

  constructor(
    query: string,
    columns: Column[],
    resource: ResourceInfo,
    functions: Record<string, Function>
  ) {
    this.queryText = query;
    this.columns = columns;
    this.resource = resource;
    this.functions = functions;
  }

  public get columnNames(): string[] {
    return this.columns.map(col => col.name);
  }

  public get columnTypes(): FieldType[] {
    return this.columns.map(col => col.type);
  }
}

export interface QueryResult {
  rawRows?: Record<string, any>[];
  rows?: any[];
  rowCount: number;
  query: QueryElements;
  customerId: string;
}

export interface IResultWriter {
  beginScript(scriptName: string, query: QueryElements): Promise<void>|void;
  beginCustomer(customerId: string): Promise<void>|void;
  addRow(customerId: string, parsedRow: any[], rawRow: any[]): Promise<void>|void;
  endCustomer(customerId: string): Promise<void>|void;
  endScript(): Promise<void>|void;
}
