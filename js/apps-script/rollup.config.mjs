/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import typescript from '@rollup/plugin-typescript';
import cleanup from 'rollup-plugin-cleanup';
import license from 'rollup-plugin-license';
import {fileURLToPath} from 'url';
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import alias from '@rollup/plugin-alias';
import path from 'path';
import babel from '@rollup/plugin-babel';
import replace from '@rollup/plugin-replace';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default {
  input: 'src/index.ts',
  treeshake: false,
  output: {
    dir: 'dist',
    format: 'esm',
    entryFileNames: 'code.gs',
  },
  plugins: [
    alias({
      entries: [
        {
          find: './logger.js',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/logger.ts',
        },
        {
          find: 'fs',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'path',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'url',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'fs/promises',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'node:fs',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'module',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'zlib',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'http',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'https',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'stream',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'querystring',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'assert',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'crypto',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'events',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'os',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'util',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'node:util',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'tls',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'net',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'node:events',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'node:process',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/process.ts',
        },
        {
          find: 'buffer',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: 'process',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/process.ts',
        },
        {
          find: 'child_process',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
        {
          find: '@google-cloud/storage',
          replacement:
            '/Users/segy/work/gaarf/ads-api-fetcher/js/apps-script/src/lib/stubs/empty.ts',
        },
      ],
    }),
    json(),
    resolve({
      browser: true,
      preferBuiltins: false,
    }),
    commonjs(),
    typescript({
      tsconfig: './tsconfig.json',
    }),
    {
      name: 'bigint-and-process-replacement',
      transform(code) {
        return {
          code: code
            .replace(/\b(\d+)n\b/g, 'BigInt($1)')
            .replace(/process\.env/g, '({})'),
          map: null,
        };
      },
    },
    // it's a workaround for issue in lodash (in isPrototype method) cause the library to fail on load 
    // because of this new limitation in AppsScript:
    // https://developers.devsite.corp.google.com/apps-script/guides/support/troubleshooting#prohibited-constructor-access
    replace({
      preventAssignment: true,
      delimiters: ['', ''],
      values: {
        'value && value.constructor': '(value && typeof value !== "function" ? value.constructor : null)'
      }
    }),
    babel({
      babelHelpers: 'bundled',
      presets: ['@babel/preset-env'],
    }),
    cleanup({comments: 'none', extensions: ['.ts']}),
    license({
      banner: {
        content: {
          file: fileURLToPath(new URL('license-header.txt', import.meta.url)),
        },
      },
    }),
  ],
  context: 'this',
};
