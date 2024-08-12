import express from 'express';
import { GoogleAdsApiConfig } from 'google-ads-api-report-fetcher';
import { ILogger } from './logger';
export declare function getScript(req: express.Request, logger: ILogger): Promise<{
    queryText: string;
    scriptName: string;
}>;
export declare function getAdsConfig(req: express.Request): Promise<GoogleAdsApiConfig>;
export declare function getProject(): Promise<string>;
export declare function splitIntoChunks(array: Array<any>, max: number): any[][];
export declare function setLogLevel(req: express.Request): void;
/**
 * Start a periodic logging of memory usage in backgroung.
 * @param logger logger to write to
 * @param intervalMs interval in milliseconds
 * @returns a callback to call for stopping logging
 */
export declare function startPeriodicMemoryLogging(logger: ILogger, intervalMs?: number): () => void;
