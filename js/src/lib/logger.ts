
import winston from 'winston';
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;

const {format} = winston;

/** Default log level (usualy one of 'info' or 'debug') */
export const LOG_LEVEL = argv.logLevel || process.env.LOG_LEVEL ||
    (process.env.NODE_ENV === 'production' ? 'info' : 'debug');

const colors = {
  error: 'red',
  warn: 'yellow',
  info: 'green',
  http: 'magenta',
  debug: 'white',
}

winston.addColors(colors);

const transports: winston.transport[] = [];
transports.push(new winston.transports.Console({
  format: format.combine(
      format.colorize({all: true}),
      format.printf(
          (info) => `${info.timestamp} ${info.level}: ${info.message}`,
          ),
      )
}));

const logger = winston.createLogger({
  level: LOG_LEVEL,  // NOTE: we use same log level for all transports
  format: format.combine(
      format.timestamp({format: 'YYYY-MM-DD HH:mm:ss:ms'}),
      // format to add 'component' meta value into log message (prepending
      // '[$component] ')
      winston.format((info: winston.Logform.TransformableInfo, opts?: any) => {
        if (info.component && info.message &&
            !info.message.startsWith(`[${info.component}]`)) {
          info.message = `[${info.component}] ${info.message}`;
        }
        return info;
      })()),
  transports
});
export default logger;
