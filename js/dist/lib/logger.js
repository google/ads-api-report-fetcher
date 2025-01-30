import { createLogger } from './logger-factory.js';
let logger;
export function getLogger() {
    if (!logger) {
        logger = createLogger();
    }
    return logger;
}
//# sourceMappingURL=logger.js.map