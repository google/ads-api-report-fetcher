"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LOG_LEVEL = void 0;
const winston_1 = __importDefault(require("winston"));
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;
const { format } = winston_1.default;
/** Default log level (usualy one of 'info' or 'debug') */
exports.LOG_LEVEL = argv.logLevel || process.env.LOG_LEVEL ||
    (process.env.NODE_ENV === 'production' ? 'info' : 'debug');
const colors = {
    error: 'red',
    warn: 'yellow',
    info: 'green',
    http: 'magenta',
    debug: 'white',
};
winston_1.default.addColors(colors);
const transports = [];
transports.push(new winston_1.default.transports.Console({
    format: format.combine(format.colorize({ all: true }), format.printf((info) => `${info.timestamp} ${info.level}: ${info.message}`))
}));
const logger = winston_1.default.createLogger({
    level: exports.LOG_LEVEL,
    format: format.combine(format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss:ms' }), 
    // format to add 'component' meta value into log message (prepending
    // '[$component] ')
    winston_1.default.format((info, opts) => {
        if (info.component && info.message &&
            !info.message.startsWith(`[${info.component}]`)) {
            info.message = `[${info.component}] ${info.message}`;
        }
        return info;
    })()),
    transports
});
exports.default = logger;
//# sourceMappingURL=logger.js.map