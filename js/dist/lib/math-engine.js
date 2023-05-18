"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.math_parse = void 0;
/**
 * Copyright 2023 Google LLC
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
const core_1 = require("@js-joda/core");
const mathjs_1 = require("mathjs");
const mathjs = (0, mathjs_1.create)(mathjs_1.all);
// MathJS customization:
//  - date/time support (using type from @js-joda: LocalDateTime, LocalDate,
//    Duration, Period)
mathjs.import([
    // data types
    (0, mathjs_1.factory)('LocalDateTime', ['typed'], function createLocalDateTime({ typed }) {
        typed.addType({
            name: 'LocalDateTime',
            test: (x) => x && x.constructor.name === 'LocalDateTime'
        });
        return core_1.LocalDateTime;
    }, { lazy: false }),
    (0, mathjs_1.factory)('LocalDate', ['typed'], function createLocalDate({ typed }) {
        typed.addType({
            name: 'LocalDate',
            test: (x) => x && x.constructor.name === 'LocalDate'
        });
        return core_1.LocalDate;
    }, { lazy: false }),
    (0, mathjs_1.factory)('Duration', ['typed'], function createDuration({ typed }) {
        typed.addType({
            name: 'Duration',
            test: (x) => x && x.constructor.name === 'Duration'
        });
        return core_1.Duration;
    }, { lazy: false }),
    (0, mathjs_1.factory)('Period', ['typed'], function createPeriod({ typed }) {
        typed.addType({
            name: 'Period',
            test: (x) => x && x.constructor.name === 'Period'
        });
        return core_1.Period;
    }, { lazy: false }),
    // conversion functions and factory functions
    (0, mathjs_1.factory)('datetime', ['typed'], function createLocalDateTime({ typed }) {
        return typed('datetime', {
            '': () => core_1.LocalDateTime.now(),
            'null': () => core_1.LocalDateTime.now(),
            'string': (x) => core_1.LocalDateTime.parse(x),
            'string, string': (x, format) => {
                let formatter = core_1.DateTimeFormatter.ofPattern(format);
                return core_1.LocalDateTime.parse(x, formatter);
            }
        });
    }),
    (0, mathjs_1.factory)('date', ['typed'], function createLocalDateTime({ typed }) {
        return typed('datetime', {
            '': () => core_1.LocalDate.now(),
            'null': () => core_1.LocalDate.now(),
            'string': (x) => core_1.LocalDate.parse(x),
            'string,string': (x, format) => {
                let formatter = core_1.DateTimeFormatter.ofPattern(format);
                return core_1.LocalDate.parse(x, formatter);
            },
            'LocalDateTime': (x) => x.toLocalDate(),
            'number, number, number': (a, b, c) => core_1.LocalDate.of(a, b, c)
        });
    }),
    (0, mathjs_1.factory)('duration', ['typed'], function createDuration({ typed }) {
        return typed('duration', { 'string': (x) => core_1.Duration.parse(x) });
    }),
    (0, mathjs_1.factory)('period', ['typed'], function createDuration({ typed }) {
        return typed('period', { 'string': (x) => core_1.Period.parse(x) });
    }),
    // operations with Date types
    (0, mathjs_1.factory)('add', ['typed'], function createLocalDateTimeAdd({ typed }) {
        return typed('add', {
            'LocalDateTime, Duration': (a, b) => a.plus(b),
            'LocalDate, Period': (a, b) => a.plus(b),
            'LocalDate, any': (a, b) => a.plus(core_1.Period.parse('P' + b + 'D')),
            'any, any': (a, b) => a + b
        });
    }),
    (0, mathjs_1.factory)('subtract', ['typed'], function createLocalDateTimeSubtract({ typed }) {
        return typed('subtract', {
            'LocalDateTime, Duration': (a, b) => a.minus(b),
            'LocalDate, Period': (a, b) => a.minus(b),
            'LocalDate, any': (a, b) => a.minus(core_1.Period.parse('P' + b + 'D')),
            'LocalDateTime, LocalDateTime': (a, b) => core_1.Duration.between(b, a),
            'LocalDate, LocalDate': (a, b) => core_1.Period.between(b, a),
            'any, any': (a, b) => a - b
        });
    }),
    // date format functions
    (0, mathjs_1.factory)('format', ['typed'], function createDateTimeFormat({ typed }) {
        return typed('format', {
            'LocalDate, string': (a, format) => {
                let formatter = core_1.DateTimeFormatter.ofPattern(format);
                return a.format(formatter);
            },
            'LocalDateTime, string': (a, format) => {
                let formatter = core_1.DateTimeFormatter.ofPattern(format);
                return a.format(formatter);
            }
        });
    }),
    // functions generators
    (0, mathjs_1.factory)('today', [], function createToday() {
        return () => core_1.LocalDate.now();
    }),
    (0, mathjs_1.factory)('yesterday', [], function createToday() {
        return () => core_1.LocalDate.now().minusDays(1);
    }),
    (0, mathjs_1.factory)('tomorrow', [], function createToday() {
        return () => core_1.LocalDate.now().plusDays(1);
    }),
    (0, mathjs_1.factory)('now', [], function createNow() {
        return () => core_1.LocalDateTime.now();
    })
], { override: true });
exports.math_parse = mathjs.parse;
//# sourceMappingURL=math-engine.js.map