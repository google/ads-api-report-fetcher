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
import date_add from 'date-fns/add'

import {formatDateISO, getElapsed, substituteMacros} from './../lib/utils';

suite('substituteMacros', () => {
  test('support empty params', async function() {
    let res = substituteMacros('abc={xyz}', undefined);
    assert.deepStrictEqual(res.queryText, 'abc={xyz}');
    assert.deepEqual(res.unknown_params, ['xyz']);
  });

  test('support multiple instances of same macro', () => {
    let res = substituteMacros('abc={xyz},def={xyz}', {'xyz': 123});
    assert.deepStrictEqual(res.queryText, 'abc=123,def=123');
    assert.deepEqual(res.unknown_params.length, 0);
  });

  test('process only supplied params', () => {
    let res = substituteMacros('{qqq}={xyz}', {'qqq': 'zzz'});
    assert.deepStrictEqual(res.queryText, 'zzz={xyz}');
    assert.deepEqual(res.unknown_params, ['xyz']);
  });

  test('getElapsed', () => {
    let started = new Date(2022, 5, 24, 12, 39, 44, 100);
    let now = new Date(2022, 5, 24, 12, 39, 44, 100);
    assert.equal(getElapsed(started, now), '00:00:00.000');
    assert.equal(
        getElapsed(started, new Date(2022, 5, 24, 22, 49, 45, 111)),
        '10:10:01.011');
  });

  test('expressions: empty expression', function() {
    let query_text = '\'${}\'';
    let query = substituteMacros(query_text);
    assert.deepEqual(query.queryText, '\'\'');
  });

  test('expressions: arithmetic expression', function() {
    let query_text = '${(5+5)/10}';
    let query = substituteMacros(query_text);
    assert.deepEqual(query.queryText, '1');
  });

  test('expressions: macro inside expression', function() {
    let query_text = `
        segments.date >= '$\{today() - period('P10D')}' AND
        segments.date <= '$\{today()-period('P{days_ago}D')\}'
    `.replaceAll(/[ ]{2,}/g, ' ')
                         .replaceAll(/\n/g, '')
                         .trim();
    let query = substituteMacros(query_text, {days_ago: 1});
    // we should get a query with date range: [today-10;today-1]
    let now = new Date();
    let from = formatDateISO(date_add(now, {days: -10}), '-');
    let to = formatDateISO(date_add(now, {days: -1}), '-');
    console.log(query.queryText);
    assert.deepEqual(
        query.queryText,
        `segments.date >= '${from}' AND segments.date <= '${to}'`);
  });

  test('expressions: macro as whole expressions', function() {
    let query_text = '${{macro}}';
    let query = substituteMacros(query_text, {macro: 'today()'});
    let now = formatDateISO(new Date(), '-');
    assert.deepEqual(query.queryText, now);
  });

  test('expressions: date operations', function() {
    // date factory method and minus operator:
    let query_text = '${date(2022,7,20) - period(\'P10D\')}';
    let query = substituteMacros(query_text);
    assert.deepEqual(query.queryText, '2022-07-10');

    // date generator function (today) and plus operator:
    query_text = '${today() + period(\'P2D\')}';
    query = substituteMacros(query_text);
    assert.deepEqual(
        query.queryText, formatDateISO(date_add(new Date(), {days: 2}), '-'));

    // calling method on Date object (plusMonths)
    query_text = '${date(2022,7,20).plusMonths(1)}';
    query = substituteMacros(query_text);
    assert.deepEqual(
        query.queryText, formatDateISO(new Date(2022, 7, 20), '-'));
    // note: 7 in Date means August (8) because it zero-based,
    // while in date factory function 7 is July as it's 1-based

    // plus operator for Date object and number:
    query_text = '${date(\'2022-07-01\') + 1}';
    query = substituteMacros(query_text);
    assert.deepEqual(query.queryText, '2022-07-02');

    // substruct two Dates
    query_text = "${tomorrow()-today()}";
    query = substituteMacros(query_text);
    assert.deepEqual(query.queryText, 'P1D');
  });

  test('expressions: date formating', function() {
    let query_text = "${format(date('2022-07-01'), 'yyyyMMdd')}";
    let query = substituteMacros(query_text);
    assert.deepEqual(query.queryText, '20220701');
  });

  test('expressions: accessing macro in expression', function() {
    let query_text = '${macro + 10}';
    let query = substituteMacros(query_text, {macro: 10});
    assert.deepEqual(query.queryText, '20');
  });
});
