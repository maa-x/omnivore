import * as fs from 'fs';
import * as path from 'path';
import { Storage, GetSignedUrlConfig } from '@google-cloud/storage';

interface StorageService {
  save(filePath: string, data: string | Buffer, options?: { contentType?: string; timeout?: number }): Promise<void>;
  download(filePath: string): Promise<string>;
  isFileExists(filePath: string): Promise<boolean>;
  getFiles(prefix?: string): Promise<string[]>;
  createFile(filePath: string): any;
  generateSignedUrl(filePathName: string, contentType: string, selectedBucket?: string): Promise<string>;
}

class GCSStorageService implements StorageService {
  private storage: Storage;
  private bucketName: string;

  constructor(bucketName: string, keyFilename?: string) {
    this.storage = new Storage({ keyFilename });
    this.bucketName = bucketName;
  }

  async save(filePath: string, data: string | Buffer, options?: { contentType?: string; timeout?: number }): Promise<void> {
    const file = this.storage.bucket(this.bucketName).file(filePath);
    const saveOptions = {
      metadata: {
        contentType: options?.contentType || 'application/octet-stream',
      },
      timeout: options?.timeout,
    };
    await file.save(data, saveOptions);
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

  async getFiles(prefix?: string): Promise<string[]> {
    prefix = prefix || '';
    const [files] = await this.storage.bucket(this.bucketName).getFiles({ prefix });
    return files.map(file => file.name);
  }

  createFile(filePath: string): any {
    return this.storage.bucket(this.bucketName).file(filePath);
  }

  async generateSignedUrl(filePathName: string, contentType: string, selectedBucket?: string): Promise<string> {
    // These options will allow temporary uploading of file with requested content type
    const options: GetSignedUrlConfig = {
      version: 'v4',
      action: 'write',
      expires: Date.now() + 15 * 60 * 1000, // 15 minutes
      contentType: contentType,
    }

    // Get a v4 signed URL for uploading file
    const [url] = await this.storage
      .bucket(selectedBucket || this.bucketName)
      .file(filePathName)
      .getSignedUrl(options)
    return url
  }
}

class FileSystemStorageService implements StorageService {
  private baseDirectory: string;

  constructor(baseDirectory: string) {
    this.baseDirectory = baseDirectory;
  }

  // Options are not used in FileSystemStorageService.save but are kept for compatibility with GCSStorageService
  async save(filePath: string, data: string | Buffer, _options?: { contentType?: string; timeout?: number }): Promise<void> {
    const fullFilePath = path.join(this.baseDirectory, filePath);
    fs.mkdirSync(path.dirname(fullFilePath), { recursive: true });
    if (Buffer.isBuffer(data)) {
      fs.writeFileSync(fullFilePath, data);
    } else {
      fs.writeFileSync(fullFilePath, data, { encoding: 'utf-8' });
    }
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

  async getFiles(prefix?: string): Promise<string[]> {
    try {
      var filter = prefix || '';
      const files = fs.readdirSync(this.baseDirectory);
      return files.filter(file => file.startsWith(filter));
    } catch {
      return [];
    }
  }

  createFile(filePath: string): any {
    return path.join(this.baseDirectory, filePath);
  }

  // We return a file:// URL for local file system, which should tell our app to read the file from local file system
  async generateSignedUrl(filePathName: string, _contentType: string, _selectedBucket?: string): Promise<string>  {
    return `file://${path.join(this.baseDirectory, filePathName)}`;
  }
}

/* On GAE/Prod, we shall rely on default app engine service account credentials.
 * Two changes needed: 1) add default service account to our uploads GCS Bucket
 * with create and view access. 2) add 'Service Account Token Creator' role to
 * the default app engine service account on the IAM page. We also need to
 * enable IAM related APIs on the project.
 */
export const storageService: StorageService = process.env.USE_FS_STORAGE === 'true'
  ? new FileSystemStorageService(process.env.FS_UPLOAD_PATH || '/mnt/shared/omnivore-files')
  : new GCSStorageService(process.env.GCS_UPLOAD_BUCKET || 'omnivore-files', process.env.GCS_UPLOAD_SA_KEY_FILE_PATH);

module.exports = { storageService };