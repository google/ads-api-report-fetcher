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

import {getElapsed, substituteMacros} from './../lib/utils';

suite('substituteMacros', () => {
  test('support empty params', async function () {
    let res = substituteMacros('abc={xyz}', undefined);
    assert.deepStrictEqual(res.queryText, 'abc={xyz}');
    assert.deepEqual(res.unknown_params, ['xyz']);
  });

  test('support multiple instances of same macro', () => {
    let res = substituteMacros('abc={xyz},def={xyz}', { 'xyz': 123 });
    assert.deepStrictEqual(res.queryText, 'abc=123,def=123');
    assert.deepEqual(res.unknown_params.length, 0);
  });

  test('process only supplied params', () => {
    let res = substituteMacros('{qqq}={xyz}', {'qqq': 'zzz'});
    assert.deepStrictEqual(res.queryText, 'zzz={xyz}');
    assert.deepEqual(res.unknown_params, ['xyz']);
  });

  test('support date_iso', () => {
    let res = substituteMacros('abc={date_iso}', undefined);
    let now = new Date();
    let month = now.getMonth() + 1;
    let day = now.getDate();
    let iso = now.getFullYear() +
        (month < 10 ? '0' + month.toString() : month.toString()) +
        (day < 10 ? '0' + day : day);
    assert.deepStrictEqual(res.queryText, 'abc=' + iso);
    assert.deepEqual(res.unknown_params.length, 0);
  });

  test('getElapsed', () => {
    let started = new Date(2022, 5, 24, 12, 39, 44, 100);
    let now = new Date(2022, 5, 24, 12, 39, 44, 100);
    assert.equal(getElapsed(started, now), '00:00:00.000');
    assert.equal(
        getElapsed(started, new Date(2022, 5, 24, 22, 49, 45, 111)),
        '10:10:01.011');
  })
});
