"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdsRowParser = void 0;
const google_ads_api_1 = require("google-ads-api");
const lodash_1 = __importDefault(require("lodash"));
const types_1 = require("./types");
const utils_1 = require("./utils");
class AdsRowParser {
    parseRow(row, query) {
        // flatten the tree of object into a flat array of values
        let row_values = {};
        for (let field of Object.keys(row)) {
            let item = row[field];
            (0, utils_1.traverseObject)(item, (name, value, path, object) => {
                let field_full = path.join('.');
                row_values[field_full] = value;
            }, [field]);
        }
        console.log('Flattened row:');
        console.log(row_values);
        // process customizers
        let row_values_arr = [];
        for (let i = 0; i < query.fields.length; i++) {
            let field = query.fields[i];
            let customizer = query.customizers[i];
            let value = row_values[field];
            if (customizer && customizer.type === types_1.CustomizerType.Function) {
                value = this.getValueWithCustomizer(value, customizer, query);
            }
            else if (value && customizer) {
                if (lodash_1.default.isArray(value)) {
                    let new_value = [];
                    for (let j = 0; j < value.length; j++) {
                        new_value[j] =
                            this.getValueWithCustomizer(value[j], customizer, query);
                    }
                    value = new_value;
                }
                else {
                    value = this.getValueWithCustomizer(value, customizer, query);
                }
            }
            row_values_arr.push(value);
        }
        // parse numbers as enum's field names
        for (let i = 0; i < query.fields.length; i++) {
            let field = query.fields[i];
            let value = row_values_arr[i];
            let colType = query.columnTypes[i];
            if (colType.kind === types_1.FieldTypeKind.enum && colType.repeated &&
                lodash_1.default.isArray(value)) {
                for (let j = 0; j < value.length; j++) {
                    let subval = value[j];
                    if (lodash_1.default.isNumber(subval)) {
                        let enumType = google_ads_api_1.enums[colType.typeName];
                        if (enumType) {
                            value[j] = enumType[subval];
                        }
                    }
                }
            }
            else if (colType.kind === types_1.FieldTypeKind.enum) {
                if (lodash_1.default.isNumber(value)) {
                    let enumType = google_ads_api_1.enums[colType.typeName];
                    if (enumType) {
                        row_values_arr[i] = enumType[value];
                    }
                }
            }
            else if (colType.kind === types_1.FieldTypeKind.struct) {
                if (value && value.toJSON) {
                    row_values_arr[i] = value.toJSON();
                }
            }
        }
        // for (let i = 0; i < query.fields.length; i++) {
        //   let field = query.fields[i];
        //   let value: any = row_values_arr[i];
        //   if (_.isNumber(value)) {
        //     let enumTypeName = (<any>fields.enumFields)[field];
        //     if (enumTypeName) {
        //       let enumType = (<any>enums)[enumTypeName];
        //       if (enumType) {
        //         row_values_arr[i] = enumType[value];
        //       }
        //     }
        //   }
        // }
        return row_values_arr;
    }
    getValueWithCustomizer(value, customizer, query) {
        if (!value)
            return value;
        if (customizer.type === types_1.CustomizerType.NestedField) {
            value = (0, utils_1.navigateObject)(value, customizer.selector);
        }
        else if (customizer.type === types_1.CustomizerType.ResourceIndex) {
            value = value.split('~')[customizer.index];
            if (value) {
                if (customizer.index === 0) {
                    value = value.match(/[^/]+\/(\d+)$/)[1];
                }
                value = (0, utils_1.tryParseNumber)(value);
            }
        }
        else if (customizer.type === types_1.CustomizerType.Function) {
            let func = query.functions[customizer.function];
            // TODO: move the check to ads-query-editor
            if (!func)
                throw new Error(`InvalidQuerySyntax: unknown function ${customizer.function}`);
            value = func(value);
        }
        // else if (customizer.type === CustomizerType.Pointer) {
        //   value = navigateObject(value, customizer.value);
        // }
        return value;
    }
}
exports.AdsRowParser = AdsRowParser;
//# sourceMappingURL=ads-row-parser.js.map