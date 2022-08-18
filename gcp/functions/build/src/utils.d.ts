import express from 'express';
export declare function getScript(req: express.Request): Promise<{
    queryText: string;
    scriptName: string;
}>;
