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
