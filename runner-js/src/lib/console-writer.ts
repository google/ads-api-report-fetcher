import {IResultWriter, QueryElements} from './types';

export interface ConsoleWriterOptions {}
export class ConsoleWriter implements IResultWriter {
  constructor(options?: ConsoleWriterOptions) {}
  beginScript(scriptName: string, query: QueryElements): void|Promise<void> {}
  endScript(): void|Promise<void> {}
  beginCustomer(customerId: string): void|Promise<void> {}
  endCustomer(): void|Promise<void> {}
  addRow(parsedRow: any[]): void {}
}
