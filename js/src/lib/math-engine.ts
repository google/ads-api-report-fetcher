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
import {DateTimeFormatter, Duration, LocalDate, LocalDateTime, Period} from '@js-joda/core';
import {all, create, factory, MathNode} from 'mathjs';

const mathjs = create(all);
// MathJS customization:
//  - date/time support (using type from @js-joda: LocalDateTime, LocalDate,
//    Duration, Period)
mathjs!.import!(
    [
      // data types
      factory(
          'LocalDateTime', ['typed'],
          function createLocalDateTime({typed}: {typed?: any}) {
            typed.addType({
              name: 'LocalDateTime',
              test: (x: any) => x && x.constructor.name === 'LocalDateTime'
            })
            return LocalDateTime
          },
          {lazy: false}),

      factory(
          'LocalDate', ['typed'],
          function createLocalDate({typed}: {typed?: any}) {
            typed.addType({
              name: 'LocalDate',
              test: (x: any) => x && x.constructor.name === 'LocalDate'
            })
            return LocalDate
          },
          {lazy: false}),

      factory(
          'Duration', ['typed'],
          function createDuration({typed}: {typed?: any}) {
            typed.addType({
              name: 'Duration',
              test: (x: any) => x && x.constructor.name === 'Duration'
            })
            return Duration
          },
          {lazy: false}),

      factory(
          'Period', ['typed'],
          function createPeriod({typed}: {typed?: any}) {
            typed.addType({
              name: 'Period',
              test: (x: any) => x && x.constructor.name === 'Period'
            })
            return Period
          },
          {lazy: false}),

      // conversion functions and factory functions
      factory(
          'datetime', ['typed'],
          function createLocalDateTime({typed}: {typed?: any}) {
            return typed('datetime', {
              '': () => LocalDateTime.now(),
              'null': () => LocalDateTime.now(),
              'string': (x: any) => LocalDateTime.parse(x),
              'string, string': (x: any, format: string) => {
                let formatter = DateTimeFormatter.ofPattern(format);
                return LocalDateTime.parse(x, formatter);
              }
            })
          }),

      factory(
          'date', ['typed'],
          function createLocalDateTime({typed}: {typed?: any}) {
            return typed('datetime', {
              '': () => LocalDate.now(),
              'null': () => LocalDate.now(),
              'string': (x: any) => LocalDate.parse(x),
              'string,string': (x: any, format: string) => {
                let formatter = DateTimeFormatter.ofPattern(format);
                return LocalDate.parse(x, formatter);
              },
              'LocalDateTime': (x: any) => x.toLocalDate(),
              'number, number, number': (a: any, b: any, c: any) =>
                  LocalDate.of(a, b, c)
            })
          }),

      factory(
          'duration', ['typed'],
          function createDuration({typed}: {typed?: any}) {
            return typed('duration', {'string': (x: any) => Duration.parse(x)})
          }),

      factory(
          'period', ['typed'],
          function createDuration({typed}: {typed?: any}) {
            return typed('period', {'string': (x: any) => Period.parse(x)})
          }),

      // operations with Date types
      factory(
          'add', ['typed'],
          function createLocalDateTimeAdd({typed}: {typed?: any}) {
            return typed('add', {
              'LocalDateTime, Duration': (a: any, b: any) => a.plus(b),
              'LocalDate, Period': (a: any, b: any) => a.plus(b),
              'LocalDate, any': (a: any, b: any) =>
                  a.plus(Period.parse('P' + b + 'D')),
              'any, any': (a: any, b: any) => a + b
            })
          }),

      factory(
          'subtract', ['typed'],
          function createLocalDateTimeSubtract({typed}: {typed?: any}) {
            return typed('subtract', {
              'LocalDateTime, Duration': (a: any, b: any) => a.minus(b),
              'LocalDate, Period': (a: any, b: any) => a.minus(b),
              'LocalDate, any': (a: any, b: any) =>
                  a.minus(Period.parse('P' + b + 'D')),
              'LocalDateTime, LocalDateTime': (a: any, b: any) =>
                  Duration.between(b, a),
              'LocalDate, LocalDate': (a: any, b: any) => Period.between(b, a),
              'any, any': (a: any, b: any) => a - b
            })
          }),

      // date format functions
      factory(
          'format', ['typed'],
          function createDateTimeFormat({typed}: {typed?: any}) {
            return typed('format', {
              'LocalDate, string': (a: LocalDate, format: string) => {
                let formatter = DateTimeFormatter.ofPattern(format);
                return a.format(formatter);
              },
              'LocalDateTime, string': (a: LocalDateTime, format: string) => {
                let formatter = DateTimeFormatter.ofPattern(format);
                return a.format(formatter);
              }
            })
          }),
      // functions generators
      factory(
          'today', [],
          function createToday() {
            return () => LocalDate.now();
          }),
      factory(
          'yesterday', [],
          function createToday() {
            return () => LocalDate.now().minusDays(1);
          }),
      factory(
          'tomorrow', [],
          function createToday() {
            return () => LocalDate.now().plusDays(1);
          }),
      factory(
          'now', [],
          function createNow() {
            return () => LocalDateTime.now();
          })
    ],
    {override: true});

export const math_parse = mathjs.parse!;
