/* eslint-disable @typescript-eslint/no-explicit-any */
/*
 Copyright 2025 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

declare const peg$allowedStartRules: string[];
declare function peg$SyntaxError(
  message: any,
  expected: any,
  found: any,
  location: any,
): any;
declare class peg$SyntaxError {
  constructor(message: any, expected: any, found: any, location: any);
  format(sources: any): string;
}
declare namespace peg$SyntaxError {
  function buildMessage(expected: any, found: any): string;
}
declare function peg$parse(input: any, options?: any): any;
export {
  peg$allowedStartRules as StartRules,
  peg$SyntaxError as SyntaxError,
  peg$parse as parse,
};
