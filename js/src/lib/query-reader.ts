import path from "path";
import chalk from "chalk";
import { getFileContent } from "./file-utils";
import { IQueryReader, InputQuery } from "./types";
import { getLogger } from "./logger";
import { globSync } from "glob";

export class FileQueryReader implements IQueryReader {
  scripts: string[];
  logger;

  constructor(scripts: string[] | undefined) {
    this.scripts = [];
    if (scripts && scripts.length) {
      for (let script of scripts) {
        if (script.includes("*") || script.includes("**")) {
          const expanded_files = globSync(script);
          this.scripts.push(...expanded_files);
        } else {
          script = script.trim();
          if (script) this.scripts.push(script);
        }
      }
    }
    this.logger = getLogger();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<InputQuery, any, undefined> {
    for (const script of this.scripts) {
      let queryText = await getFileContent(script);
      this.logger.info(`Processing query from ${chalk.gray(script)}`);
      let scriptName = path.basename(script).split(".sql")[0];
      const item = { name: scriptName, text: queryText };
      yield item;
    }
  }
}

export class ConsoleQueryReader implements IQueryReader {
  scripts: string[];
  logger;

  constructor(scripts: string[] | undefined) {
    this.scripts = scripts || [];
    this.logger = getLogger();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<InputQuery, any, undefined> {
    let i = 0;
    for (let script of this.scripts) {
      i++;
      let scriptName = "query" + i;
      let match = script.match(/^([\d\w]+)\:/);
      if (match && match.length > 1) {
        scriptName = match[1];
        script = script.substring(scriptName.length + 1);
      }
      this.logger.info(`Processing inline query ${scriptName}:\n ${chalk.gray(script)}`);
      const item = { name: scriptName, text: script };
      yield item;
    }
  }
}
