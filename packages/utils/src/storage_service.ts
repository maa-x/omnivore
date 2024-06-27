import * as fs from 'fs';
import * as path from 'path';
import { Storage } from '@google-cloud/storage';

interface StorageService {
    save(filePath: string, data: string): Promise<void>;
    download(filePath: string): Promise<string>;
    isFileExists(filePath: string): Promise<boolean>;
  }

class GCSStorageService implements StorageService {
  private storage: Storage;
  private bucketName: string;

  constructor(bucketName: string, keyFilename?: string) {
    this.storage = new Storage({ keyFilename });
    this.bucketName = bucketName;
  }

  async save(filePath: string, data: string): Promise<void> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    await file.save(data);
  }

  async download(filePath: string): Promise<string> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    const downloadResponse = await file.download();
    return downloadResponse[0].toString('utf-8');
  }

  async isFileExists(filePath: string): Promise<boolean> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    const existsResponse = await file.exists();
    return existsResponse[0];
  }
}

class FileSystemStorageService implements StorageService {
  private baseDirectory: string;

  constructor(baseDirectory: string) {
    this.baseDirectory = baseDirectory;
  }

  async save(filePath: string, data: string): Promise<void> {
    const fullFilePath = path.join(this.baseDirectory, filePath);
    fs.mkdirSync(path.dirname(fullFilePath), { recursive: true });
    fs.writeFileSync(fullFilePath, data);
  }

  async download(filePath: string): Promise<string> {
    const fullFilePath = path.join(this.baseDirectory, filePath);
    return fs.promises.readFile(fullFilePath, { encoding: 'utf-8' });
  }

  async isFileExists(filePath: string): Promise<boolean> {
    const fullFilePath = path.join(this.baseDirectory, filePath);
    try {
      await fs.promises.access(fullFilePath);
      return true;
    } catch {
      return false;
    }
  }
}

export const storageService: StorageService = process.env.USE_FS_STORAGE === 'true'
    ? new FileSystemStorageService(process.env.FS_UPLOAD_PATH || '/tmp/omnivore-files')
    : new GCSStorageService(process.env.GCS_UPLOAD_BUCKET || 'omnivore-files', process.env.GCS_UPLOAD_SA_KEY_FILE_PATH);

module.exports = {storageService};