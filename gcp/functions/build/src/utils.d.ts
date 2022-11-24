import express from 'express';
import { GoogleAdsApiConfig } from 'google-ads-api-report-fetcher';
export declare function getScript(req: express.Request): Promise<{
    queryText: string;
    scriptName: string;
}>;
export declare function getAdsConfig(req: express.Request): Promise<GoogleAdsApiConfig>;
