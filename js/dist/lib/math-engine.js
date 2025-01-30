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
import { DateTimeFormatter, Duration, LocalDate, LocalDateTime, Period, } from '@js-joda/core';
import { all, create, factory } from 'mathjs';
/* eslint-disable @typescript-eslint/no-explicit-any */
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
export const math_parse = mathjs.parse;
//# sourceMappingURL=math-engine.js.map