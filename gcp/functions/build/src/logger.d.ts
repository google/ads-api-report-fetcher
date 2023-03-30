import express from 'express';
export interface ILogger {
    info(message: string, aux?: any): Promise<void>;
    warn(message: string, aux?: any): Promise<void>;
    error(message: string, aux?: any): Promise<void>;
}
export declare function createLogger(req: express.Request, projectId: string, component: string): ILogger;
