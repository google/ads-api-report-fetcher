import {Storage} from '@google-cloud/storage';

export async function getFileFromGCS(filePath: string): Promise<string> {
  let parsed = new URL(filePath);
  let bucket = parsed.hostname;
  let filename = parsed.pathname.substring(1);

  return new Promise((resolve, reject) => {
    const storage = new Storage();
    let fileContents = Buffer.from('');
    storage.bucket(bucket)
        .file(filename)
        .createReadStream()
        .on('error',
            (err) => {
              reject(
                  `Failed to download '${filePath}' file content from GCS: ` +
                  err);
            })
        .on('data',
            (chunk) => {
              fileContents = Buffer.concat([fileContents, chunk]);
            })
        .on('end', () => {
          let content = fileContents.toString('utf8');
          resolve(content);
        });
  });
}
