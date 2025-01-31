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
/* eslint-disable @typescript-eslint/no-explicit-any */
import { DateTimeFormatter, Duration, LocalDate, LocalDateTime, Period, } from '@js-joda/core';
import { all, create, factory, isAccessorNode, isConstantNode, isOperatorNode, isParenthesisNode, isRangeNode, isSymbolNode, } from 'mathjs';
export const mathjs = create(all);
// MathJS customization:
//  - date/time support (using type from @js-joda: LocalDateTime, LocalDate,
//    Duration, Period)
mathjs.import([
    // data types
    factory('LocalDateTime', ['typed'], ({ typed }) => {
        typed.addType({
            name: 'LocalDateTime',
            test: (x) => x && x.constructor.name === 'LocalDateTime',
        });
        return LocalDateTime;
    }, { lazy: false }),
    factory('LocalDate', ['typed'], ({ typed }) => {
        typed.addType({
            name: 'LocalDate',
            test: (x) => x && x.constructor.name === 'LocalDate',
        });
        return LocalDate;
    }, { lazy: false }),
    factory('Duration', ['typed'], ({ typed }) => {
        typed.addType({
            name: 'Duration',
            test: (x) => x && x.constructor.name === 'Duration',
        });
        return Duration;
    }, { lazy: false }),
    factory('Period', ['typed'], ({ typed }) => {
        typed.addType({
            name: 'Period',
            test: (x) => x && x.constructor.name === 'Period',
        });
        return Period;
    }, { lazy: false }),
    // conversion functions and factory functions
    factory('datetime', ['typed'], ({ typed }) => {
        return typed('datetime', {
            '': () => LocalDateTime.now(),
            null: () => LocalDateTime.now(),
            string: (x) => LocalDateTime.parse(x),
            'string, string': (x, format) => {
                const formatter = DateTimeFormatter.ofPattern(format);
                return LocalDateTime.parse(x, formatter);
            },
        });
    }),
    factory('date', ['typed'], ({ typed }) => {
        return typed('datetime', {
            '': () => LocalDate.now(),
            null: () => LocalDate.now(),
            string: (x) => LocalDate.parse(x),
            'string,string': (x, format) => {
                const formatter = DateTimeFormatter.ofPattern(format);
                return LocalDate.parse(x, formatter);
            },
            LocalDateTime: (x) => x.toLocalDate(),
            'number, number, number': (a, b, c) => LocalDate.of(a, b, c),
        });
    }),
    factory('duration', ['typed'], ({ typed }) => {
        return typed('duration', { string: (x) => Duration.parse(x) });
    }),
    factory('period', ['typed'], ({ typed }) => {
        return typed('period', { string: (x) => Period.parse(x) });
    }),
    // operations with Date types
    factory('add', ['typed'], ({ typed }) => {
        return typed('add', {
            'LocalDateTime, Duration': (a, b) => a.plus(b),
            'LocalDate, Period': (a, b) => a.plus(b),
            'LocalDate, any': (a, b) => a.plus(Period.parse('P' + b + 'D')),
            'any, any': (a, b) => a + b,
        });
    }),
    factory('subtract', ['typed'], ({ typed }) => {
        return typed('subtract', {
            'LocalDateTime, Duration': (a, b) => a.minus(b),
            'LocalDate, Period': (a, b) => a.minus(b),
            'LocalDate, any': (a, b) => a.minus(Period.parse('P' + b + 'D')),
            'LocalDateTime, LocalDateTime': (a, b) => Duration.between(b, a),
            'LocalDate, LocalDate': (a, b) => Period.between(b, a),
            'any, any': (a, b) => a - b,
        });
    }),
    // date format functions
    factory('format', ['typed'], ({ typed }) => {
        return typed('format', {
            'LocalDate, string': (a, format) => {
                const formatter = DateTimeFormatter.ofPattern(format);
                return a.format(formatter);
            },
            'LocalDateTime, string': (a, format) => {
                const formatter = DateTimeFormatter.ofPattern(format);
                return a.format(formatter);
            },
        });
    }),
    // functions generators
    factory('today', [], () => {
        return () => LocalDate.now();
    }),
    factory('yesterday', [], () => {
        return () => LocalDate.now().minusDays(1);
    }),
    factory('tomorrow', [], () => {
        return () => LocalDate.now().plusDays(1);
    }),
    factory('now', [], () => {
        return () => LocalDateTime.now();
    }),
], { override: true });
// Helper to build full property access chain
function getFullPropertyChain(node) {
    if (!node)
        return null;
    if (isRangeNode(node)) {
        // For RangeNode, combine left and right parts with ":"
        const leftPart = getFullPropertyChain(node.start);
        const rightPart = getFullPropertyChain(node.end);
        return leftPart && rightPart ? `${leftPart}:${rightPart}` : null;
    }
    else if (isAccessorNode(node)) {
        if (node.index.dotNotation) {
            const objectChain = getFullPropertyChain(node.object);
            if (objectChain) {
                // Get field name from the IndexNode's dimensions
                const fieldName = node.index.dimensions[0]
                    .value;
                if (Number.isFinite(fieldName)) {
                    return objectChain;
                }
                else {
                    return `${objectChain}.${fieldName}`;
                }
            }
        }
        else {
            // For array access, process the object part only
            return getFullPropertyChain(node.object);
        }
    }
    else if (isSymbolNode(node)) {
        return node.name;
    }
    return null;
}
/**
 * Extract all field accesses from a mathjs expression.
 * @param node a mathjs ast node
 * @returns list of field accessors
 */
export function extractFieldAccesses(node) {
    const fieldAccesses = new Set();
    const processedNodes = new Set();
    // Helper to find field access in function chain
    const findFieldAccessInFunctionChain = (node) => {
        if (!node)
            return null;
        // For a FunctionNode, we need to:
        // 1. Process its arguments
        // 2. Look at its fn property which should be an AccessorNode
        // 3. Follow fn.object which might be another FunctionNode or the actual field access
        if (node.type === 'FunctionNode') {
            const funcNode = node;
            // Process function arguments
            if (funcNode.args) {
                funcNode.args.forEach(arg => traverse(arg));
            }
            // If fn is an AccessorNode, check its object
            if (funcNode.fn && funcNode.fn.type === 'AccessorNode') {
                if (funcNode.fn.object) {
                    // If object is a FunctionNode, continue the chain
                    if (funcNode.fn.object.type === 'FunctionNode') {
                        return findFieldAccessInFunctionChain(funcNode.fn.object);
                    }
                    // If object is an AccessorNode, this is our field access
                    else if (funcNode.fn.object.type === 'AccessorNode') {
                        return getFullPropertyChain(funcNode.fn.object);
                    }
                    // Handle ParenthesisNode and other types by traversing them
                    else {
                        traverse(funcNode.fn.object);
                    }
                }
            }
        }
        return null;
    };
    // Main traversal function
    const traverse = (node) => {
        if (!node || processedNodes.has(node))
            return;
        processedNodes.add(node);
        //console.log(node);
        // Handle AccessorNode chains
        if (isAccessorNode(node)) {
            const chain = getFullPropertyChain(node);
            if (chain) {
                fieldAccesses.add(chain);
            }
        }
        if (isRangeNode(node)) {
            const chain = getFullPropertyChain(node);
            if (chain) {
                fieldAccesses.add(chain);
            }
        }
        // TODO: Handle array access
        // else if (isIndexNode(node)) {
        //   if (node.object) {
        //     traverse(node.object);
        //   }
        // }
        // Handle standalone SymbolNodes
        else if (isSymbolNode(node) && !isConstantNode(node)) {
            fieldAccesses.add(node.name);
        }
        // Handle function calls
        else if (node.type === 'FunctionNode') {
            // For function calls, check if it's a property access (a method call)
            const chain = findFieldAccessInFunctionChain(node);
            if (chain) {
                fieldAccesses.add(chain);
            }
        }
        else if (isOperatorNode(node) && node.args) {
            // Continue traversal for any other children
            node.args.forEach(arg => traverse(arg));
        }
        // Handle ParenthesisNode
        else if (isParenthesisNode(node) && node.content) {
            traverse(node.content);
        }
    };
    // Start traversal from root node
    traverse(node);
    return Array.from(fieldAccesses);
}
export const math_parse = mathjs.parse;
//# sourceMappingURL=math-engine.js.map