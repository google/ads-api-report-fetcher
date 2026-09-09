/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Cloud Run Job entrypoint for Gaarf
 * Executes long-running Ads queries outside Cloud Workflows HTTP timeout.
 */
import {executeGaarfQuery, GaarfTaskArgs} from './gaarf.js';
import {getProject, getMemoryUsage, startPeriodicMemoryLogging} from './utils.js';
import {createLogger} from './logger.js';

async function runJob() {
  const projectId = await getProject();
  const logger = createLogger(null, projectId, 'gaarf-job');
  logger.info('Starting Gaarf Cloud Run Job execution');

  let rawPayload = process.env.GAARF_PAYLOAD;
  if (!rawPayload && process.argv.length > 2) {
    rawPayload = process.argv[2];
  }

  if (!rawPayload) {
    logger.error(
      'Missing payload. Please provide GAARF_PAYLOAD environment variable or command-line argument.'
    );
    process.exit(1);
  }

  let args: GaarfTaskArgs;
  try {
    args = JSON.parse(rawPayload);
  } catch (err: any) {
    logger.error('Failed to parse GAARF_PAYLOAD as JSON', {
      error: err.message,
      rawPayload,
    });
    process.exit(1);
  }

  const dumpMemory = !!(process.env.DUMP_MEMORY);
  let dispose;
  if (dumpMemory) {
    logger.info(getMemoryUsage('Start'));
    dispose = startPeriodicMemoryLogging(logger, 60_000);
  }

  try {
    const result = await executeGaarfQuery(args, logger, 'gaarf-job');
    logger.info('Gaarf Cloud Run Job completed successfully', {result});

    if (args.callbackUrl) {
      logger.info(`Sending callback to: ${args.callbackUrl}`);
      try {
        await fetch(args.callbackUrl, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(result),
        });
      } catch (cbErr: any) {
        logger.error('Failed to send callback to Cloud Workflows', {
          error: cbErr.message,
        });
      }
    }

    process.exit(0);
  } catch (err: any) {
    logger.error('Gaarf Cloud Run Job failed', {
      error: err.message,
      stack: err.stack,
    });
    process.exit(1);
  } finally {
    if (dumpMemory) {
      if (dispose) dispose();
      logger.info(getMemoryUsage('End'));
    }
  }
}

runJob();
