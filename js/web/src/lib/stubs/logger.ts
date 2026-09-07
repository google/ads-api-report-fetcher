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

export interface ILogger {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  info(message: string, ...meta: any[]): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  warn(message: string, ...meta: any[]): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(message: string, ...meta: any[]): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug(message: string, ...meta: any[]): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  verbose(message: string, ...meta: any[]): void;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type LogListener = (level: string, message: string, meta: any[]) => void;

class WebLogger implements ILogger {
  private listener?: LogListener;

  setListener(listener?: LogListener) {
    this.listener = listener;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  info(message: string, ...meta: any[]) {
    if (this.listener) this.listener('info', message, meta);
    console.info(message, ...meta);
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  warn(message: string, ...meta: any[]) {
    if (this.listener) this.listener('warn', message, meta);
    console.warn(message, ...meta);
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(message: string, ...meta: any[]) {
    if (this.listener) this.listener('error', message, meta);
    console.error(message, ...meta);
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug(message: string, ...meta: any[]) {
    if (this.listener) this.listener('debug', message, meta);
    if (console.debug) {
      console.debug(message, ...meta);
    } else {
      console.log(message, ...meta);
    }
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  verbose(message: string, ...meta: any[]) {
    if (this.listener) this.listener('verbose', message, meta);
    console.log(message, ...meta);
  }
}

const logger = new WebLogger();

export function getLogger(): ILogger {
  return logger;
}

export function setLogListener(listener?: LogListener) {
  logger.setListener(listener);
}
