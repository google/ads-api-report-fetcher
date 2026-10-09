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
import {fileURLToPath} from 'url';
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import alias from '@rollup/plugin-alias';
import replace from '@rollup/plugin-replace';
import terser from '@rollup/plugin-terser';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const emptyStub = path.resolve(__dirname, 'src/lib/stubs/empty.ts');
const loggerStub = path.resolve(__dirname, 'src/lib/stubs/logger.ts');
const processStub = path.resolve(__dirname, 'src/lib/stubs/process.ts');
const schemaLoaderWeb = path.resolve(
  __dirname,
  'src/lib/ads-api-schema-loader-web.ts',
);

export default {
  input: {
    index: 'src/index.ts',
    'schema/v25': 'src/lib/bundled-schema.ts',
  },
  output: [
    {
      dir: 'dist',
      format: 'esm',
      entryFileNames: '[name].js',
      chunkFileNames: 'chunks/[name]-[hash].js',
      sourcemap: true,
    },
  ],
  plugins: [
    alias({
      entries: [
        {
          find: /^\.\.?\/ads-api-schema-loader-rest(\.js)?$/,
          replacement: schemaLoaderWeb,
        },
        {
          find: /^\.\.?\/logger(\.js)?$/,
          replacement: loggerStub,
        },
        {
          find: /^google-auth-library$/,
          replacement: emptyStub,
        },
        {
          find: /^@google-cloud\/.*/,
          replacement: emptyStub,
        },
        {
          find: /^winston$/,
          replacement: emptyStub,
        },
        {
          find: /^fs\/promises$/,
          replacement: emptyStub,
        },
        {
          find: /^node:fs$/,
          replacement: emptyStub,
        },
        {
          find: /^fs$/,
          replacement: emptyStub,
        },
        {
          find: /^path$/,
          replacement: emptyStub,
        },
        {
          find: /^url$/,
          replacement: emptyStub,
        },
        {
          find: /^module$/,
          replacement: emptyStub,
        },
        {
          find: /^zlib$/,
          replacement: emptyStub,
        },
        {
          find: /^http$/,
          replacement: emptyStub,
        },
        {
          find: /^https$/,
          replacement: emptyStub,
        },
        {
          find: /^stream$/,
          replacement: emptyStub,
        },
        {
          find: /^querystring$/,
          replacement: emptyStub,
        },
        {
          find: /^assert$/,
          replacement: emptyStub,
        },
        {
          find: /^crypto$/,
          replacement: emptyStub,
        },
        {
          find: /^node:events$/,
          replacement: emptyStub,
        },
        {
          find: /^events$/,
          replacement: emptyStub,
        },
        {
          find: /^os$/,
          replacement: emptyStub,
        },
        {
          find: /^node:util$/,
          replacement: emptyStub,
        },
        {
          find: /^util$/,
          replacement: emptyStub,
        },
        {
          find: /^tls$/,
          replacement: emptyStub,
        },
        {
          find: /^net$/,
          replacement: emptyStub,
        },
        {
          find: /^buffer$/,
          replacement: emptyStub,
        },
        {
          find: /^child_process$/,
          replacement: emptyStub,
        },
        {
          find: /^node:process$/,
          replacement: processStub,
        },
        {
          find: /^process$/,
          replacement: processStub,
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
    replace({
      preventAssignment: true,
      values: {
        'process.env.NODE_ENV': JSON.stringify('production'),
        'process.env': '({})',
      },
    }),
    terser(),
    {
      name: 'emit-root-dts',
      generateBundle() {
        this.emitFile({
          type: 'asset',
          fileName: 'index.d.ts',
          source: "export * from './web/src/index.js';\n",
        });
      },
    },
  ],
};
