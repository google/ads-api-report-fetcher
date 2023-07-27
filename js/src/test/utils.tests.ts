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

import assert from 'assert';
import date_add from 'date-fns/add';

import {formatDateISO, getElapsed, substituteMacros, renderTemplate} from './../lib/utils';
import { render } from 'nunjucks';

suite('substituteMacros', () => {
  test('support empty params', async function() {
    let res = substituteMacros('abc={xyz}', undefined);
    assert.deepStrictEqual(res.text, 'abc={xyz}');
    assert.deepEqual(res.unknown_params, ['xyz']);
  });

  test('support multiple instances of same macro', () => {
    let res = substituteMacros('abc={xyz},def={xyz}', {'xyz': 123});
    assert.deepStrictEqual(res.text, 'abc=123,def=123');
    assert.deepEqual(res.unknown_params.length, 0);
  });

  test('process only supplied params', () => {
    let res = substituteMacros('{qqq}={xyz}', {'qqq': 'zzz'});
    assert.deepStrictEqual(res.text, 'zzz={xyz}');
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
    assert.deepEqual(query.text, '\'\'');
  });

  test('expressions: arithmetic expression', function() {
    let query_text = '${(5+5)/10}';
    let query = substituteMacros(query_text);
    assert.deepEqual(query.text, '1');
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
    console.log(query.text);
    assert.deepEqual(
        query.text,
        `segments.date >= '${from}' AND segments.date <= '${to}'`);
  });

  test('expressions: macro as whole expressions', function() {
    let query_text = '${{macro}}';
    let query = substituteMacros(query_text, {macro: 'today()'});
    let now = formatDateISO(new Date(), '-');
    assert.deepEqual(query.text, now);
  });

  test('expressions: date operations', function() {
    // date factory method and minus operator:
    let query_text = '${date(2022,7,20) - period(\'P10D\')}';
    let query = substituteMacros(query_text);
    assert.deepEqual(query.text, '2022-07-10');

    // date generator function (today) and plus operator:
    query_text = '${today() + period(\'P2D\')}';
    query = substituteMacros(query_text);
    assert.deepEqual(
        query.text, formatDateISO(date_add(new Date(), {days: 2}), '-'));

    // calling method on Date object (plusMonths)
    query_text = '${date(2022,7,20).plusMonths(1)}';
    query = substituteMacros(query_text);
    assert.deepEqual(
        query.text, formatDateISO(new Date(2022, 7, 20), '-'));
    // note: 7 in Date means August (8) because it zero-based,
    // while in date factory function 7 is July as it's 1-based

    // plus operator for Date object and number:
    query_text = '${date(\'2022-07-01\') + 1}';
    query = substituteMacros(query_text);
    assert.deepEqual(query.text, '2022-07-02');

    // substruct two Dates
    query_text = "${tomorrow()-today()}";
    query = substituteMacros(query_text);
    assert.deepEqual(query.text, 'P1D');
  });

  test('expressions: date formating', function() {
    let query_text = "${format(date('2022-07-01'), 'yyyyMMdd')}";
    let query = substituteMacros(query_text);
    assert.deepEqual(query.text, '20220701');
  });

  test('expressions: accessing macro in expression', function() {
    let query_text = '${macro + 10}';
    let query = substituteMacros(query_text, {macro: 10});
    assert.deepEqual(query.text, '20');
  });

  test('macro with dynamic dates: YYYYMMDD', function() {
    let query_text = '{start_date}';
    // 7 days from todays
    let query = substituteMacros(query_text, {start_date: ':YYYYMMDD-7'});
    let expected = formatDateISO(date_add(new Date(), {days: -7}), '-');
    assert.deepEqual(query.text, expected);
    // today
    query = substituteMacros(query_text, { start_date: ':YYYYMMDD' });
    expected = formatDateISO(new Date(), '-');
    assert.deepEqual(query.text, expected);
  });

  test('macro with dynamic dates: YYYYMM', function() {
    let query_text = '{start_date}';
    // 1 month from todays
    let query = substituteMacros(query_text, {start_date: ':YYYYMM - 1'});
    let expected = formatDateISO(date_add(new Date(), {months: -1}), '-');
    assert.deepEqual(query.text, expected);
  });

  test('macro with dynamic dates: YYYY', function() {
    let query_text = '{start_date}';
    // 1 year from todays
    let query = substituteMacros(query_text, {start_date: ':YYYY - 1'});
    let expected = formatDateISO(date_add(new Date(), {years: -1}), '-');
    assert.deepEqual(query.text, expected);
  });

  test("support magic date macros: date_iso", () => {
    let res = substituteMacros("abc={date_iso}", undefined);
    let now = new Date();
    let month = now.getMonth() + 1;
    let day = now.getDate();
    let iso =
      now.getFullYear() +
      (month < 10 ? "0" + month.toString() : month.toString()) +
      (day < 10 ? "0" + day : day);
    assert.deepStrictEqual(res.text, "abc=" + iso);
    assert.deepEqual(res.unknown_params.length, 0);

    // now the same but with override for date_iso
    res = substituteMacros("abc={date_iso}", { date_iso: "20230501" });
    assert.deepStrictEqual(res.text, "abc=20230501");
    assert.deepEqual(res.unknown_params.length, 0);
  });

  test("support magic date macros: current_date", () => {
    const res = substituteMacros("abc={current_date}");
    const now = new Date();
    const month = now.getMonth() + 1;
    const day = now.getDate();
    const iso =
      now.getFullYear() +
      "-" +
      (month < 10 ? "0" + month.toString() : month.toString()) +
      "-" +
      (day < 10 ? "0" + day : day);
    assert.deepStrictEqual(res.text, "abc=" + iso);
  });

  test("support magic date macros: current_datetime", () => {
    const res = substituteMacros("abc={current_datetime}");
    const now = new Date();
    const month = now.getMonth() + 1;
    const day = now.getDate();
    const hours = now.getHours();
    const minutes = now.getMinutes();
    const seconds = now.getSeconds();
    const iso =
      now.getFullYear() +
      "-" +
      (month < 10 ? "0" + month.toString() : month.toString()) +
      "-" +
      (day < 10 ? "0" + day : day) + " " +
      (hours < 10 ? "0" + hours.toString() : hours.toString()) + ":" +
      (minutes < 10 ? "0" + minutes.toString() : minutes.toString()) + ":" +
      (seconds < 10 ? "0" + seconds.toString() : seconds.toString());
    assert.deepStrictEqual(res.text, "abc=" + iso);
  });
});

suite('renderTemplate', () => {
  test("support empty params", async function () {
    let res = renderTemplate("abc={xyz}", undefined);
    assert.deepStrictEqual(res, "abc={xyz}");
  });

  test("template with if-else", () => {
    const template =
      "SELECT field_one, {% if key == 'field_2' %}field_two{% else %}field_three{% endif %} FROM some_table";

    assert.equal(
      renderTemplate(template, {}),
      "SELECT field_one, field_three FROM some_table"
    );
    assert.equal(
      renderTemplate(template, { key: "field_2" }),
      "SELECT field_one, field_two FROM some_table"
    );
    assert.equal(
      renderTemplate(template, { key: "field_3" }),
      "SELECT field_one, field_three FROM some_table"
    );
  })

  test("template with for loop", () => {
    const template =
      "SELECT field_one, {% for day in cohort_days %}{{day}} AS day_{{day}}, {% endfor %}FROM some_table";

    assert.equal(
      renderTemplate(template, { "cohort_days": "1,2" }),
      "SELECT field_one, 1 AS day_1, 2 AS day_2, FROM some_table"
    );
    assert.equal(
      renderTemplate(template, { cohort_days: [1,2] }),
      "SELECT field_one, 1 AS day_1, 2 AS day_2, FROM some_table"
    );
    assert.equal(
      renderTemplate(template, {  }),
      "SELECT field_one, FROM some_table"
    );
  });
});

