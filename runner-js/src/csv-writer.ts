import csvStringify from 'csv-stringify';
import {stringify} from 'csv-stringify/sync';
import fs from 'fs';
import path from 'path';

import {IResultWriter, QueryElements, QueryResult} from './types';


export class CsvWriter implements IResultWriter {
  destination: string;
  filename: string|undefined;
  appending = false;
  customerRows = 0;
  rows: any[][] = [];
  query: QueryElements|undefined;

  constructor(destination: string) {
    this.destination = destination;
  }

  beginScript(scriptName: string, query: QueryElements) {
    this.appending = false;
    this.query = query;

    let filename = scriptName + '.csv';
    if (this.destination) {
      if (!fs.existsSync(this.destination)) {
        fs.mkdirSync(this.destination, {recursive: true});
      }
      filename = path.join(this.destination, filename);
    }
    this.filename = filename;
    if (fs.existsSync(this.filename)) {
      fs.rmSync(this.filename);
    }
  }

  endScript() {
    this.filename = undefined;
  }
  beginCustomer(customerId: string) {
    this.rows = [];
  }
  endCustomer() {
    let csvOptions: csvStringify.Options = {
      header: !this.appending,
      quoted: false,
      columns: this.query!.columnNames,
      cast: {
        boolean: (value: boolean, context: csvStringify.CastingContext) =>
            value ? 'true' : 'false'
      }
    };
    let csv = stringify(this.rows, csvOptions);
    fs.writeFileSync(
        this.filename!, csv,
        {encoding: 'utf-8', flag: this.appending ? 'a' : 'w'});

    if (this.rows.length > 0) {
      console.log(
          (this.appending ? 'Updated ' : 'Created ') + this.filename +
          ` with ${this.rows.length} rows`);
    }

    this.appending = true;
    this.rows = [];
  }

  addRow(parsedRow: any[]) {
    if (!parsedRow || parsedRow.length == 0) return;
    this.rows.push(parsedRow);
  }
}
